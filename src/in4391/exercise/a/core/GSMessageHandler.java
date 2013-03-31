package in4391.exercise.a.core;

import in4391.exercise.a.messages.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Message parsing thread
 */
public class GSMessageHandler extends Thread
{
    private BlockingQueue<AbstractMessage> queue = new LinkedBlockingQueue<AbstractMessage>();
    private GSNode parent;

    public GSMessageHandler(GSNode parent)
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

    public void queue(AbstractMessage m)
    {
        queue.add(m);
    }

    public void handleMessage(AbstractMessage m)
    {
        if (parent.getMaster() && m.getTimestamp() == -1) {
            // Broadcast only regular messages for now
            System.out.println("Master: Received " + m.getClass().getSimpleName() + ", for " + m.getReceiver());

            parent.masterBroadcast(m);
        } else {
            // Log the message if it's timestamped and interesting.
            if ((m.getTimestamp() != -1 || m instanceof MasterMessage || m instanceof MasterElectionMessage) && !(m instanceof Message)) {
                parent.getMessageLog().add(m);
            }

            // Copy the clock if we're not the master
            if (m.getTimestamp() != -1 && !parent.getMaster()) {
                parent.setClock(m.getTimestamp());
            }

            // Skip handling the message if it's not for us or we sent it unless it's one of [GSJoinMessage, MasterMessage, MasterElectionMessage]
            if (m.PARSE_TYPE == MessageParseType.SYSTEM) {
                // Message directed at every GS
                callMessageHandler(m);
            } else if (m.PARSE_TYPE == MessageParseType.MASTER && parent.getMaster()) {
                // Messages for the master node
                callMessageHandler(m);
            } else if (m.getReceiver() == null && m.PARSE_TYPE == MessageParseType.GLOBAL) {
                // Message that can be global but also directed
                callMessageHandler(m);
            } else if (parent.getId().equals(m.getReceiver()) && !parent.getId().equals(m.getSender())) {
                // Directed messages
                callMessageHandler(m);
            } else {
                //System.out.println("Ignoring message for some reason " + m.toString());
                return;
            }
        }
    }

    private void callMessageHandler(AbstractMessage m)
    {
        System.out.println("GSNode: Received " + m.getClass().getSimpleName() + ", from " + m.getSender());
        if (m instanceof GSJoinMessage)
            parent.handle((GSJoinMessage) m);
        else if (m instanceof GSLeaveMessage)
            parent.handle((GSLeaveMessage) m);
        else if (m instanceof MasterMessage)
            parent.handle((MasterMessage) m);
        else if (m instanceof MasterElectionMessage)
            parent.handle((MasterElectionMessage) m);
        else if (m instanceof LogStateMessage)
            parent.handle((LogStateMessage) m);
        else if (m instanceof Message)
            parent.handle((Message) m);
        else if (m instanceof JobAddMessage)
            parent.handle((JobAddMessage) m);
        else if (m instanceof JobDoneMessage)
            parent.handle((JobDoneMessage) m);
        else if (m instanceof JobOffloadMessage)
            parent.handle((JobOffloadMessage) m);
        else if (m instanceof GSDeadMessage)
            parent.handle((GSDeadMessage) m);
        else if (m instanceof RMJoinMessage)
            parent.handle((RMJoinMessage) m);
        else if (m instanceof MatchMessage)
            parent.handle((MatchMessage) m);
        else if (m instanceof RMLeaveMessage)
            parent.handle((RMLeaveMessage) m);
        else if (m instanceof JobAssignMessage)
            parent.handle((JobAssignMessage) m);
        else
            System.out.println("Warning: unknown message type!");
    }

    public int getQueueSize()
    {
        return queue.size();
    }
}
