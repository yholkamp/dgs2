package in4391.exercise.a.wantds;

import in4391.exercise.a.core.RMNode;

import java.util.ArrayList;
import java.util.List;

/**
 * The Cluster class represents a single cluster in the virtual grid system. It consists of a
 * collection of nodes and a resource manager.
 *
 * @author Niels Brouwers
 */
public class Cluster implements Runnable
{
    public static final int SIZE = 50;
    private List<Node> nodes;
    private RMNode resourceManager;

    // polling frequency (ms)
    private long pollSleep = 100;

    // polling thread
    private Thread pollingThread;
    private boolean running;

    /**
     * Creates a new Cluster, with a number of nodes and a resource manager
     *
     * @param nodeCount the number of nodes in this cluster
     * @param rmNode
     */
    public Cluster(int nodeCount, RMNode rmNode)
    {
        resourceManager = rmNode;
        nodes = new ArrayList<Node>(nodeCount);

        // Initialize the nodes
        for (int i = 0; i < nodeCount; i++) {
            // Make nodes report their status to us
            Node n = new Node(resourceManager);

            nodes.add(n);
        }

        // Start the polling thread
        running = true;
        pollingThread = new Thread(this);
        pollingThread.start();
    }

    /**
     * Returns the number of nodes in this cluster.
     *
     * @return the number of nodes in this cluster
     */
    public int getNodeCount()
    {
        return nodes.size();
    }

    /**
     * Finds a free node and returns it. If no free node can be found, the method returns null.
     *
     * @return a free Node object, or null if no such node can be found.
     */
    public Node getFreeNode()
    {
        // Find a free node among the nodes in our cluster
        for (Node node : nodes)
            if (node.getStatus() == NodeStatus.Idle) return node;

        // if we haven't returned from the function here, we haven't found a suitable node
        // so we just return null
        return null;
    }

    public int getFreeNodeCount() {
        int count = 0;
        for (Node node : nodes)
            if (node.getStatus() == NodeStatus.Idle) count++;
        return count;
    }

    /**
     * Polling thread runner. This function polls each node in the system repeatedly. Polling
     * is needed to make each node check its internal state - whether a running job is
     * finished for instance.
     */
    public void run()
    {
        while (running) {
            // poll the nodes
            for (Node node : nodes) {
                node.poll();
            }

            resourceManager.scheduleJobs();

            // sleep
            try {
                Thread.sleep(pollSleep);
            } catch (InterruptedException ex) {
                assert (false) : "Cluster poll thread was interrupted";
            }
        }
    }

    /**
     * Stops the polling thread. This must be called explicitly to make sure the program
     * terminates cleanly.
     */
    public void stopPollThread()
    {
        running = false;
        try {
            pollingThread.join();
        } catch (InterruptedException ex) {
            assert (false) : "Cluster stopPollThread was interrupted";
        }
    }
}
