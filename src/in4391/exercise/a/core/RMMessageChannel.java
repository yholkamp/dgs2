package in4391.exercise.a.core;

import in4391.exercise.a.messages.AbstractMessage;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Message channel of an RM to some GS node.
 */
public class RMMessageChannel extends Thread
{
    private BlockingQueue<AbstractMessage> queue = new LinkedBlockingQueue<AbstractMessage>();
    private IGSNode targetNode;
    private String targetName;

    public RMMessageChannel()
    {
    }

    /**
     * Queues a message for future delivery.
     * @param message
     */
    public void sendMessage(AbstractMessage message)
    {
        queue.add(message);
    }

    public void run()
    {
        AbstractMessage message;
        try {
            // Take a new message from the queue or wait.
            while (!isInterrupted()) {
                if (targetNode == null) {
                    Thread.sleep(100);
                    continue;
                }

                message = queue.peek();
                if (message != null) {
                    try {
                        System.out.println("Delivering message " + message.toString());
                        targetNode.receive(message);
                        queue.remove();
                    } catch (Exception e) {
                        System.err.println("RMMessageChannel to " + targetName + ", exception: " + e.toString());
                        Thread.sleep(1000);
                    }
                } else {
                    Thread.sleep(100);
                }
            }
        } catch (InterruptedException e) {
            System.out.println("RMMessageChannel " + targetName + " just got killed.");
        }
    }

    public void setTargetNode(IGSNode targetNode)
    {
        this.targetNode = targetNode;
    }

    public void setTargetName(String targetName)
    {
        this.targetName = targetName;
    }
}
