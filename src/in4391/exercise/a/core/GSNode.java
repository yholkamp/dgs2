package in4391.exercise.a.core;

import com.sun.management.OperatingSystemMXBean;
import in4391.exercise.a.messages.*;
import in4391.exercise.a.util.ConnectionRegistry;
import in4391.exercise.a.wantds.Job;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The main class representing a GS node, handles messaging and the overall glue between the different parts
 */
public class GSNode implements IGSNode
{
    private String id = null;
    private GridScheduler scheduler;
    private Map<String, GSMessageChannel> channelMap;
    private GSMessageChannel masterChannel;
    private ConcurrentMap<String, Queue<Job>> jobQueues;
    private ConcurrentMap<String, Set<String>> matchedRMs;
    private ConcurrentMap<String, Set<Job>> rmJobSets;
    private int clock = 0;
    private boolean master = false;
    private String masterName = null;
    private ConnectionRegistry registry;
    private GSMessageHandler messageHandler;
    // Poller to check if RM nodes are alive (run by the master node)
    private AlivePoller alivePoller;
    private BufferedWriter logWriter;
    /**
     * FIFO-ordered thread-safe list containing the history of events as sent by the master
     */
    private Queue<AbstractMessage> messageLog;
    private HashSet<String> nodeSet;

    public GSNode(String pid, Registry registry, String rmiHost)
    {
        this.id = pid;

        channelMap = new HashMap<String, GSMessageChannel>();
        masterChannel = new GSMessageChannel(this);
        masterChannel.start();
        messageLog = new LinkedList<AbstractMessage>();
        messageHandler = new GSMessageHandler(this);
        messageHandler.start();
        nodeSet = new HashSet<String>();
        this.registry = new ConnectionRegistry(registry, rmiHost);

        // Storage for job queues for all GS nodes; create at least our own
        jobQueues = new ConcurrentHashMap<String, Queue<Job>>();
        jobQueues.put(getId(), new LinkedList<Job>());

        matchedRMs = new ConcurrentHashMap<String, Set<String>>();
        rmJobSets = new ConcurrentHashMap<String, Set<Job>>();

        // Start the scheduler
        scheduler = new GridScheduler(this);
        scheduler.start();

        // Open up a file handler to write the job history on the master node
        File file = new File(id + ".job.log");
        try {
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
            logWriter = new BufferedWriter(fw);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("** Started " + pid + " **");
    }

    /**
     * Initializes the list of channels to all processes we retrieve from the RMI registry
     *
     * @throws RemoteException
     */
    public void init() throws RemoteException, AlreadyBoundException
    {
        // Make the new GSNode available through RMI
        IGSNode myStub = (IGSNode) UnicastRemoteObject.exportObject(this, 0);
        registry.bind(getId(), myStub);

        System.out.println(getGSCount());
        if (getGSCount() == 1) {
            System.out.println("I am declaring myself master and ruler of the gridniverse.\n");
            claimMastership();
        } else {
            // Broadcast hello world
            GSJoinMessage m = new GSJoinMessage();
            m.setSender(getId());
            System.out.println("Sending JoinMessage");

            broadcast(m);
        }
    }

    /**
     * Broadcasts a message as master node to all known processes after sequencing the message.
     */
    public void masterBroadcast(AbstractMessage message)
    {
        // Sequence the message
        incrementClock();
        message.setTimestamp(clock);

        // And send it to everyone, including ourselves
        broadcast(message);
        receive(message);
    }

    /**
     * Broadcasts a message to all other processes we know.
     *
     * @param message msg object to broadcast
     */
    public void broadcast(AbstractMessage message, boolean includeRMs)
    {
        System.out.println("Broadcasting message " + message + " as " + (master ? "master" : "regular"));
        try {
            String[] nodes = new String[0];
            if (master) {
                nodes = nodeSet.toArray(nodes);
            } else {
                nodes = registry.list().toArray(nodes);
            }
            // Call the handleMessage() method of every known node

            for (String process : nodes) {
                if (process.equals(id))
                    continue;

                // Include RMs?
                if (!includeRMs && process.substring(0, 2).equals("RM"))
                    continue;

                // Possibly add a missing channel
                GSMessageChannel thread = getOrCreateChannel(process);

                thread.sendMessage(message);
            }
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
    }

    /**
     * Short-hand.
     */
    public void broadcast(AbstractMessage message)
    {
        broadcast(message, false);
    }

    /**
     * Directly send a message, bypassing the master node.
     *
     * @param message
     */
    public void sendDirect(AbstractMessage message)
    {
        // Sequence the message
        incrementClock();
        message.setTimestamp(clock);

        // Construct message
        message.setSender(getId());

        getOrCreateChannel(message.getReceiver()).sendMessage(message);
    }

    /**
     * Sends a message by handing it to the master node, which will broadcast it.
     *
     * @param message
     * @throws RemoteException
     */
    public void send(AbstractMessage message)
    {
        System.out.println("Sending message " + message);
        if (message.getReceiver() == null) {
            System.out.println("No receiver set for message " + message);
            System.exit(1);
        }
        // Construct message
        message.setSender(getId());

        // If we're not the master, hand the msg to the master node for sequencing, otherwise broadcast it directly
        if (!master) {
            // Send it to the master node
            // TODO: detect missing channel
            masterChannel.sendMessage(message);
        } else {
            masterBroadcast(message);
        }
    }

    /**
     * Retrieves an existing communications channel or sets up a new one for message passing.
     * @param name
     * @return
     */
    public GSMessageChannel getOrCreateChannel(String name)
    {
        if (!channelMap.containsKey(name)) {
            registry.list();
            addMessageChannel(name, (INode) registry.lookup(name));
        }

        return channelMap.get(name);
    }

    /**
     * Either receives a message or passes it on with a sequence number if the node is currently 'master'
     *
     * @param m
     */
    public void receive(AbstractMessage m)
    {
        messageHandler.queue(m);
    }

    public void handle(LogStateMessage m)
    {
        System.out.println("Received log state " + m);
        messageLog = m.getLog();
        matchedRMs = m.getMatchedRMs();
        jobQueues = m.getJobQueues();
        jobQueues.put(getId(), new LinkedList<Job>());
    }

    public void handle(MasterMessage m)
    {
        // Reset any residual Obama 2012 artifacts
        System.out.println("Received masterMessage " + m);
        if (id.equals(m.getSender())) {
            System.out.println("** We're the new master! **");
            // We're the new master
            claimMastership();
        }
        masterName = m.getSender();
        master = id.equals(m.getSender());

        masterChannel.setTargetName(masterName);
        try {
            registry.list();
            masterChannel.setTargetNode((INode) registry.lookup(masterName));
        } catch (Exception e) {
            System.out.println("Failed to look up the new master.");
            e.printStackTrace();
        }
    }

    public void handle(Message m)
    {
        System.out.println("Received message from " + m.getSender());
        System.out.println(" - " + m.getMessage());
        // TODO: handle message
    }

    public void handle(GSJoinMessage m)
    {
        System.out.println("Received joinMessage from " + m.getSender());

        if (master) {
            // Send a message announcing us to be the master node
            sendMasterMessage(m.getSender());

            // And update the node with log info
            sendLogState(m.getSender());

            nodeSet.add(m.getSender());
            // TODO: Offload work from busy GSNodes to the new node.
        }
    }

    public void handle(GSLeaveMessage m)
    {
        System.out.println("Received leaveMessage for " + m.getName());
        removeChannel(m.getName());
    }

    public void handle(MasterElectionMessage m)
    {
        System.out.println("Received masterElectionMessage from " + m.getSender());

        String lowestIdGSName = getGSWithSmallestId();

        if (getId().equals(lowestIdGSName)) {
            System.out.println("We have the lowest ID");
            String minimalLoadGS = getLeastLoadedGSNode();

            System.out.println("** And the new master is.... " + minimalLoadGS + ", notifying the world about this **");
            MasterMessage message = new MasterMessage();
            message.setSender(minimalLoadGS);
            receive(message);
            broadcast(message, true); // Send to all nodes, including RMs

            GSDeadMessage gsm = new GSDeadMessage();
            gsm.setSender(getId());
            gsm.setReceiver(getId());
            gsm.setDeadNode(m.getDeadMaster());
            masterBroadcast(gsm);
        }
    }

    public void handle(GSDeadMessage m)
    {
        System.out.println("Received GSDeadMessage from " + m.getSender());

        fillInForDeadNode(m.getDeadNode(), m.getReceiver());
    }

    public void handle(JobAddMessage m)
    {
        String rm = m.getSender();
        if (!rmJobSets.containsKey(rm)) {
            rmJobSets.put(rm, new HashSet<Job>());
        }

        Set<Job> jobSet = rmJobSets.get(rm);
        jobSet.add(m.getJob());

        if (master) {
            logJob(m);
        }
    }

    public void handle(JobDoneMessage m)
    {
        String rm = m.getSender();
        if (rmJobSets.containsKey(rm)) {
            rmJobSets.get(rm).remove(m.getJob());
        }

        if (master) {
            logJob(m);
        }
    }

    public void handle(JobOffloadMessage m)
    {
        Job job = m.getJob();
        String matchedGSNode = getMatchedGSNodeFor(m.getSender());
        System.out.println("Received JobOffloadMessage for " + matchedGSNode);

        // Update the relevant GS node queue
        if (!jobQueues.containsKey(matchedGSNode)) {
            Queue<Job> jobQueue = new LinkedList<Job>();
            jobQueues.put(matchedGSNode, jobQueue);
        }
        jobQueues.get(matchedGSNode).add(job);
        
        if (master) {
            logJob(m);
        }
    }

    public void handle(RMJoinMessage m)
    {
        String rm = m.getSender();

        if (master) {
            // Perform matchmaking: assign this RM to the least loaded GS node
            String gsNode = getLeastLoadedGSNode();

            MatchMessage message = new MatchMessage();
            message.setSender(getId());
            message.setGSNode(gsNode);
            message.setRMNode(rm);
            masterBroadcast(message);

            getOrCreateChannel(rm).sendMessage(message);
        }

        rmJobSets.put(rm, new HashSet<Job>());
    }

    public void handle(MatchMessage m)
    {
        // There's a new match, so update our match list

        String rmNode = m.getRMNode();
        String gsNode = m.getGSNode();
        System.out.println("New match: " + gsNode + " to " + rmNode);

        if (!matchedRMs.containsKey(gsNode)) {
            matchedRMs.put(gsNode, new HashSet<String>());
        }

        Set<String> matched = matchedRMs.get(gsNode);
        matched.add(rmNode);
    }

    public void handle(RMLeaveMessage m)
    {
        String rm = m.getName();

        // Find the responsible (matched) GS node
        String matchedGSNode = getMatchedGSNodeFor(rm);

        // Recover for the crashed RM

        if (rmJobSets.containsKey(rm)) {
            if (!jobQueues.containsKey(matchedGSNode)) {
                jobQueues.put(matchedGSNode, new LinkedList<Job>());
            }

            // Add all the RMs jobs to the responsible GS node's job queue
            Queue<Job> jobQueue = jobQueues.get(matchedGSNode);
            for (Job job : rmJobSets.get(rm)) {
                jobQueue.add(job);
            }
        }

        // Delete all references to this RM
        rmJobSets.remove(rm);
        matchedRMs.get(matchedGSNode).remove(rm);
    }

    public void handle(JobAssignMessage m)
    {
        System.out.println("Assigned job: " + m);

        String assignerGS = m.getSender();
        String assigneeRM = m.getReceiver();
        Job job = m.getJob();

        // First, remove the job from the GS node queue

        // Ignore if:
        // - we sent this message, then our queue has already been updated
        // - the relevant job queue does not exist
        if (!assignerGS.equals(getId())
                && jobQueues.containsKey(assignerGS)
                && jobQueues.get(assignerGS) != null) {
            Queue<Job> queue = jobQueues.get(assignerGS);
            queue.remove(job);
        }

        // Second, update the RM queue with this new job
        if (rmJobSets.containsKey(assigneeRM)) {
            rmJobSets.get(assigneeRM).add(job);
        }

        if (master) {
            logJob(m);
        }
    }

    private void claimMastership()
    {
        master = true;
        masterName = id;

        // Store a list of currently active processes to detect failing nodes
        nodeSet = getProcessSet();

        alivePoller = new AlivePoller(this, registry);
        alivePoller.start();
    }

    /**
     * Take over all the work for the recently deceased node.
     */
    private void fillInForDeadNode(String deadNode, String target)
    {
        System.out.println("** Filling in " + target + " for " + deadNode + " **");

        // Take over all jobs in the dead node's job queue
        Queue<Job> deadQueue = getJobQueueFor(deadNode);
        Queue<Job> targetQueue = getJobQueueFor(target);
        System.out.println("   Target gets " + deadQueue.size() + " jobs");
        //TODO: interleave the two queues for lower average schedule time
        targetQueue.addAll(deadQueue);
        deadQueue.clear();

        // Assign all orphaned RMs to this GS node
        //TODO: would be more efficient to distribute these RMs over all GS nodes
        Set<String> orphanRMs = getMatchedRMsFor(deadNode);
        Set<String> targetMatches = getMatchedRMsFor(target);
        System.out.println("   Target gets " + orphanRMs.size() + " RMs");
        targetMatches.addAll(orphanRMs);
        matchedRMs.remove(deadNode);
    }

    /**
     * Generates a set of the ids of all currently available nodes.
     * @return
     */
    private HashSet<String> getProcessSet()
    {
        HashSet<String> list = new HashSet<String>();
        for (String node : registry.list()) {
            if (node.equals(id) || node.substring(0, 2).equals("RM"))
                continue;

            list.add(node);
        }
        return list;
    }

    /**
     * Get the GS node with the lowest load.
     */
    private String getLeastLoadedGSNode()
    {
        // Initially assume we have the lowest load
        int minLoad = calculateLoad();
        String minGSNode = getId();

        try {
            for (String node : registry.list()) {
                if (node.equals(id) || !node.substring(0, 2).equals("GS"))
                    continue;

                IGSNode gsNode = (IGSNode) registry.lookup(node);
                int load = gsNode.calculateLoad();

                if (load < minLoad || load <= minLoad && minGSNode.compareTo(node) == -1) {
                    minLoad = load;
                    minGSNode = node;
                }
            }
        } catch (RemoteException e) {
            e.printStackTrace();
        }

        return minGSNode;
    }

    /**
     * Sends target a message informing him we're the master node.
     * @param target
     */
    private void sendMasterMessage(String target)
    {
        MasterMessage masterMessage = new MasterMessage();
        masterMessage.setReceiver(target);
        sendDirect(masterMessage);
    }

    /**
     * Sends the target node the current system state.
     * @param target
     */
    private void sendLogState(String target)
    {
        LogStateMessage logStateMessage = new LogStateMessage();
        logStateMessage.setLog(messageLog);
        logStateMessage.setMatchedRMs(matchedRMs);
        logStateMessage.setJobQueues(jobQueues);
        logStateMessage.setReceiver(target);
        sendDirect(logStateMessage);
    }

    /**
     * Some slightly long JavaDoc comment that won't be read in the near future.
     *
     * @param name
     * @param stub
     */
    private void addMessageChannel(String name, INode stub)
    {
        GSMessageChannel thread = new GSMessageChannel(stub, name, this);
        thread.start();
        channelMap.put(name, thread);
    }

    /**
     * Atomic increment of the system clock
     */
    private synchronized void incrementClock()
    {
        clock++;
    }

    public synchronized void setClock(int clock)
    {
        this.clock = clock;
    }

    /**
     * Returns the bind string of this node
     *
     * @return
     */
    public String getId()
    {
        return id;
    }

    /**
     * Stop method, interrupts any running threads for a clean exit.
     */
    public void stop()
    {
        messageHandler.interrupt();

        try {
            System.out.println("Closing logwriter");
            logWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (String process : channelMap.keySet()) {
            GSMessageChannel thread = channelMap.get(process);
            thread.interrupt();
            scheduler.stopThread();
        }
    }

    /**
     * Announces a GS node identified by deadNode as dead.
     *
     * @param deadNode
     */
    public void markGSNodeAsDead(String deadNode)
    {
        System.out.println("* Oops! We noticed " + deadNode + " is dead.");
        removeChannel(deadNode);

        if (master) {
            // Remove the node from our node list
            System.out.println("Removing " + deadNode + " from our nodeSet");
            nodeSet.remove(deadNode);

            // And notify everyone
            GSLeaveMessage m = new GSLeaveMessage();
            m.setName(deadNode);
            m.setSender(id);
            masterBroadcast(m);

            String victim = getLeastLoadedGSNode();
            sendRecoveryElectionMessage(deadNode, victim);
        } else if (deadNode.equals(masterName)) {
            // Stop the master channel
            masterChannel.clearTarget();
            // Master node died, start election
            sendMasterElectionMessage(deadNode);
        }
    }

    /**
     * Announces an RM node identified by targetName as dead.
     *
     * @param targetName
     */
    public void markRMNodeAsDead(String targetName)
    {
        System.out.println("* Oops! We noticed " + targetName + " is dead.");
        removeChannel(targetName);

        if (master) {
            // Notify everyone
            RMLeaveMessage m = new RMLeaveMessage();
            m.setName(targetName);
            m.setSender(id);
            masterBroadcast(m);
        }
    }

    /**
     * Store our load and send our election message
     */
    private void sendMasterElectionMessage(String deadMaster)
    {
        MasterElectionMessage m = new MasterElectionMessage();
        m.setSender(id);
        m.setDeadMaster(deadMaster);
        broadcast(m);
        receive(m);
    }

    /**
     * Store our load and send our election message
     */
    private void sendRecoveryElectionMessage(String deadNode, String target)
    {
        GSDeadMessage m = new GSDeadMessage();
        m.setSender(id);
        m.setReceiver(target);
        m.setDeadNode(deadNode);
        masterBroadcast(m);
    }

    private String getMatchedGSNodeFor(String rm)
    {
        //TODO: optimize this, maybe create a cached index from RM -> GS
        String matchedGSNode = null;
        for (String gsNode : matchedRMs.keySet()) {
            Set<String> rms = getMatchedRMsFor(gsNode);
            if (rms.contains(rm)) {
                matchedGSNode = gsNode;
            }
        }

        if (matchedGSNode == null) {
            System.out.println("This shouldn't happen... (getMatchedGSNodeFor)");
            Thread.dumpStack();
            System.exit(1);
        }

        return matchedGSNode;
    }

    /**
     * Provides a load estimate of this GS through a combination of the RM node load and additional load for being the master node.
     * @return
     */
    public int calculateLoad()
    {
        int load = 1;
        for (String rm : getMatchedRMs()) {
            load += 10 + getLoadForRM(rm);
        }

        // We're the master so we're extra burdened
        if (master) {
            load = load * 1000;
        }

        return load;
    }

    /**
     * Retrieves various numbers useful for system diagnostics and monitoring.
     * @return
     * @throws RemoteException
     */
    @Override
    public int[] getNodeState() throws RemoteException
    {
        int cpuLoad = -1, processLoad = -1;
        if (System.getProperty("java.version").substring(0, 3).equals("1.7")) {
            OperatingSystemMXBean operatingSystemMXBean = (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();

            try {
                Method m = operatingSystemMXBean.getClass().getMethod("getSystemCpuLoad");
                m.setAccessible(true);
                cpuLoad = (int) (100 * (Double) m.invoke(operatingSystemMXBean));
                m = operatingSystemMXBean.getClass().getMethod("getProcessCpuLoad");
                m.setAccessible(true);
                processLoad = (int) (100 * (Double) m.invoke(operatingSystemMXBean));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return new int[]{clock, messageHandler.getQueueSize(), getJobQueue().size(), getMatchedRMs().size(), cpuLoad, processLoad};
    }

    /**
     * Remove a channel to the node identified by targetName and interrupt the thread
     *
     * @param targetName
     */
    public void removeChannel(String targetName)
    {
        GSMessageChannel channel = channelMap.remove(targetName);
        if (channel != null) {
            channel.interrupt();
        }
    }

    public void printChannels()
    {
        System.out.println("Channels for " + id);
        for (String name : channelMap.keySet()) {
            System.out.println(name);
        }
    }

    public boolean getMaster()
    {
        return master;
    }

    public Queue<Job> getJobQueue()
    {
        return getJobQueueFor(getId());
    }

    public Queue<Job> getJobQueueFor(String gsNode)
    {
        if (!jobQueues.containsKey(gsNode)) {
            jobQueues.put(gsNode, new LinkedList<Job>());
        }

        return jobQueues.get(gsNode);
    }

    public Set<String> getAllRMs()
    {
        return rmJobSets.keySet();
    }

    public Set<String> getMatchedRMs()
    {
        return getMatchedRMsFor(getId());
    }

    public Set<String> getMatchedRMsFor(String gsNode)
    {
        if (!matchedRMs.containsKey(gsNode)) {
            matchedRMs.put(gsNode, new HashSet<String>());
        }

        return matchedRMs.get(gsNode);
    }

    public int getLoadForRM(String rm)
    {
        int load = 0;
        if (rmJobSets.containsKey(rm)) {
            load = rmJobSets.get(rm).size();
        }
        return load;
    }

    public Queue<AbstractMessage> getMessageLog()
    {
        return messageLog;
    }

    public String getGSWithSmallestId()
    {
        String name = id;

        for (String process : registry.list()) {
            if (process.substring(0, 2).equals("GS") && process.compareTo(name) == -1) {
                name = process;
            }
        }

        return name;
    }

    public int getGSCount()
    {
        int count = 0;
        for (String process : registry.list()) {
            if (process.substring(0, 2).equals("GS")) {
                count++;
            }
        }
        return count;
    }

    public String getMasterName()
    {
        return masterName;
    }

    /**
     * Used by the AlivePoller to see if we're still responding.
     */
    public boolean alive()
    {
        return true;
    }

    public void writeLog()
    {
        File file = new File(id + ".system.log");

        try {
            if (!file.exists()) {
                file.createNewFile();
            }

            FileWriter fw = new FileWriter(file.getAbsoluteFile(), false);
            BufferedWriter bw = new BufferedWriter(fw);

            for (AbstractMessage m : messageLog) {
                if (m instanceof AbstractJobMessage) {
                    bw.write(((AbstractJobMessage) m).toString() + "\n");
                } else {
                    bw.write(m.toString() + "\n");
                }
            }

            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void logJob(AbstractJobMessage m)
    {
        try {
            String data = m.getClass().getSimpleName() + "," + m.getJob().getId() + "," + m.getJob().getClusterId() + "," + System.currentTimeMillis() + "\n";
            logWriter.write(data);
            logWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
