package in4391.exercise.a.core;

import com.sun.management.OperatingSystemMXBean;
import in4391.exercise.a.messages.*;
import in4391.exercise.a.util.ConnectionRegistry;
import in4391.exercise.a.wantds.Cluster;
import in4391.exercise.a.wantds.Job;
import in4391.exercise.a.wantds.JobStatus;
import in4391.exercise.a.wantds.Node;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Resource manager of a cluster.
 */
public class RMNode implements IRMNode
{
    public static int MAX_QUEUE_SIZE = 50; // Used to be 32
    private final ConnectionRegistry registry;
    private final String id;
    private final Cluster cluster;
    private final RMMessageChannel messageChannel;
    private final RMMessageHandler messageHandler;
    private Queue<Job> jobQueue;
    private int jobQueueSize;
    private String masterName;

    public RMNode(String id, Registry registry, String rmiHost)
    {
        this.id = id;
        this.registry = new ConnectionRegistry(registry, rmiHost);
        jobQueue = new ConcurrentLinkedQueue<Job>();
        this.cluster = new Cluster(Cluster.SIZE, this);

        // Initialize our message channel without GS
        messageChannel = new RMMessageChannel();
        messageChannel.start();
        messageHandler = new RMMessageHandler(this);
        messageHandler.start();

        // Number of jobs in the queue must be larger than the number of nodes, because
        // jobs are kept in queue until finished. The queue is a bit larger than the
        // number of nodes for efficiency reasons - when there are only a few more jobs than
        // nodes we can assume a node will become available soon to handle that job.
        jobQueueSize = cluster.getNodeCount() + MAX_QUEUE_SIZE;

        System.out.println(" ** We're " + id + " **");
    }

    /**
     * Initializes the list of channels to all processes we retrieve from the RMI registry
     *
     * @throws RemoteException
     */
    public void init() throws RemoteException, AlreadyBoundException
    {
        // Make the new GSNode available through RMI
        IRMNode myStub = (IRMNode) UnicastRemoteObject.exportObject(this, 0);
        registry.bind(getId(), myStub);

        connectToGridScheduler();
    }

    /**
     * Send a message to our master node
     *
     * @param m
     */
    private void send(AbstractMessage m)
    {
        m.setSender(id);
        m.setReceiver(masterName);
        messageChannel.sendMessage(m);
    }

    /**
     * Add a job to the resource manager. If there is a free node in the cluster the job will be
     * scheduled onto that Node immediately. If all nodes are busy the job will be put into a local
     * queue. If the local queue is full, the job will be offloaded to the grid scheduler.
     * <DL>
     * <DT><B>Preconditions:</B>
     * <DD>the parameter <CODE>job</CODE> cannot be null
     * <DD>a grid scheduler url has to be set for this rm before calling this function (the RM has to be
     * connected to a grid scheduler)
     * </DL>
     *
     * @param job the Job to run
     */
    public void addJob(Job job)
    {
        // If the job queue is full, offload the job to the grid scheduler
        if (jobQueue.size() >= jobQueueSize) {
            System.out.println("Offloading job");
            JobOffloadMessage m = new JobOffloadMessage(job);
            send(m);
        } else {
            queueJob(job);

            // Notify the GS that we have a new job
            JobAddMessage m = new JobAddMessage();
            m.setJob(job);
            send(m);
        }
    }

    private void queueJob(Job job)
    {
        System.out.println("Queueing job");
        // Otherwise store it in the local queue
        jobQueue.add(job);
    }

    /**
     * Tries to find a waiting job in the jobqueue.
     *
     * @return
     */
    public Job getWaitingJob()
    {
        // find a waiting job
        for (Job job : jobQueue)
            if (job.getStatus() == JobStatus.Waiting)
                return job;

        // no waiting jobs found, return null
        return null;
    }

    /**
     * Tries to schedule jobs in the jobqueue to free nodes.
     */
    public void scheduleJobs()
    {
        // while there are jobs to do and we have nodes available, assign the jobs to the
        // free nodes
        Node freeNode;
        Job waitingJob;

        while (((waitingJob = getWaitingJob()) != null) && ((freeNode = cluster.getFreeNode()) != null)) {
            freeNode.startJob(waitingJob);
        }
    }

    /**
     * Called when a job is finished
     */
    public void jobDone(Job job)
    {
        System.out.println(job + " is done.");
        // job finished, remove it from our pool
        jobQueue.remove(job);

        // Notify the GS that we finished our job
        JobDoneMessage m = new JobDoneMessage();
        m.setJob(job);
        m.setSender(id);
        send(m);
    }

    /**
     * Connect to a grid scheduler
     */
    public void connectToGridScheduler()
    {
        RMJoinMessage message = new RMJoinMessage();
        message.setSender(id);
        broadcastToGS(message);
    }

    /**
     * Broadcasts a message to all GS nodes in the system
     *
     * @param m
     */
    public void broadcastToGS(AbstractMessage m)
    {
        System.out.println("RM: Broadcasting message " + m);
        try {
            // Just sending the message with a blocking RMI call as the receive method of these processes is short,
            // non-blocking and we execute this only once per RM start.
            for (String process : registry.list()) {
                if (process.equals(id) || process.substring(0, 2).equals("RM"))
                    continue;

                IGSNode node = (IGSNode) registry.lookup(process);
                node.receive(m);
            }
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
    }

    public void handle(MatchMessage message)
    {
        System.out.println("** Discovered that the master GS is " + message);
        masterName = message.getSender();
        setMaster(masterName);
    }

    public void handle(MasterMessage message)
    {
        System.out.println("** Discovered that the master GS is " + message);
        masterName = message.getSender();
        setMaster(masterName);
    }

    public void handle(JobAssignMessage m)
    {
        queueJob(m.getJob());
    }

    public void receive(AbstractMessage message)
    {
        messageHandler.queue(message);
    }

    public void setMaster(String masterName)
    {
        messageChannel.setTargetName(masterName);
        registry.list();
        messageChannel.setTargetNode((IGSNode) registry.lookup(masterName));
    }

    public void stop()
    {
        messageChannel.interrupt();
        messageHandler.interrupt();
        cluster.stopPollThread();
    }

    public String getId()
    {
        return id;
    }

    /**
     * Used by the AlivePoller to see if we're still responding.
     */
    public boolean alive()
    {
        return true;
    }

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

        return new int[]{messageHandler.getQueueSize(), jobQueue.size(), jobQueueSize, cluster.getFreeNodeCount(), cluster.getNodeCount(), cpuLoad, processLoad};
    }

    public Queue<Job> getJobQueue()
    {
        return jobQueue;
    }
}
