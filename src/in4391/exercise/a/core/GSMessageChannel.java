package in4391.exercise.a.core;

import in4391.exercise.a.messages.AbstractMessage;
import in4391.exercise.a.messages.Message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Message channel of a GS node to some other node (RM or GS).
 */
public class GSMessageChannel extends Thread
{

    private INode targetNode;
    private String targetName;
    private BlockingQueue<AbstractMessage> queue = new LinkedBlockingQueue<AbstractMessage>();
    private GSNode parent;
    private boolean trackTime;
    private long lastMessage = System.currentTimeMillis();

    // Time (in ms) without communication after which we will check if the master is still alive
    static final int MASTER_CHECK_INTERVAL = 5*1000;

    public GSMessageChannel(INode targetNode, String targetName, GSNode parent)
    {
        this.targetNode = targetNode;
        this.targetName = targetName;
        this.parent = parent;
    }

    public GSMessageChannel(GSNode parent)
    {
        this.parent = parent;
        this.trackTime = true;
    }

    public void sendMessage(AbstractMessage message)
    {
        queue.add(message);
//        deliver(message);
    }

    public void run()
    {
        AbstractMessage message;
        try {
            // Take a new message from the queue or wait.
            while (!isInterrupted()) {
                if (targetName == null || targetNode == null) {
                    // Master is dead
                    Thread.sleep(100);
                    continue;
                }

                message = queue.peek();

                if (message == null) {
                    // Check if the last message was more than MASTER_CHECK_INTERVAL ms ago
                    if (trackTime && lastMessage < System.currentTimeMillis() - MASTER_CHECK_INTERVAL) {
                        message = new Message("Status check message");
                        message.setSender(parent.getId());
                        message.setReceiver(targetName);
                        queue.add(message);
                    } else {
                        Thread.sleep(100);
                        continue;
                    }
                }

                lastMessage = System.currentTimeMillis();
                deliver(message);
            }
        } catch (InterruptedException e) {
            System.out.println("GSMessageChannel " + targetName + " just got killed.");
        }
        System.out.println("Politely exiting");
    }

    private void deliver(AbstractMessage message)
    {
        System.out.println("Delivering " + message);
        try {
            targetNode.receive(message);
            queue.remove();
        } catch (Exception e) {
            System.err.println("GSMessageChannel: exception: " + e.toString());
            System.out.println("Occured while handling " + message);
            System.out.println("Deregistering channel to " + targetName);

            if (targetName.substring(0, 2).equals("GS")) {
                parent.markGSNodeAsDead(targetName);
            } else if (targetName.substring(0, 2).equals("RM")) {
                parent.markRMNodeAsDead(targetName);
            }
        }
    }

    public void setTargetNode(INode targetNode)
    {
        this.targetNode = targetNode;
    }

    public void setTargetName(String targetName)
    {
        this.targetName = targetName;
    }

    public void clearTarget()
    {
        this.targetName = null;
        this.targetNode = null;
    }
}
