package in4391.exercise.a.core;

import in4391.exercise.a.messages.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Message parsing thread
 */
public class RMMessageHandler extends Thread
{
    private BlockingQueue<AbstractMessage> queue = new LinkedBlockingQueue<AbstractMessage>();
    private RMNode parent;

    public RMMessageHandler(RMNode parent)
    {
        this.parent = parent;
    }

    public void run()
    {
        AbstractMessage message;
        try {
            // Take a new message from the queue or wait.
            while (!isInterrupted() && (message = queue.take()) != null) {
                handleMessage(message);
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted GSMessageHandler");
        }
    }

    /**
     * Queues a message for parsing
     * @param m
     */
    public void queue(AbstractMessage m)
    {
        queue.add(m);
    }

    /**
     * Handles a message by calling the relevant handler method of the RMNode
     * @param message
     */
    public void handleMessage(AbstractMessage message)
    {
        System.out.println("RM: Received message " + message.toString());
        if (message instanceof MatchMessage) {
            parent.handle((MatchMessage) message);
        } else if (message instanceof MasterMessage) {
            parent.handle((MasterMessage) message);
        } else if (message instanceof JobAssignMessage) {
            parent.handle((JobAssignMessage) message);
        } else {
            System.out.println("Unknown message class " + message.toString());
        }
    }

    public int getQueueSize()
    {
        return queue.size();
    }
}
