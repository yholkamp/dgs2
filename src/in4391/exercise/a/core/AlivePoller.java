package in4391.exercise.a.core;

import in4391.exercise.a.util.ConnectionRegistry;

import java.util.Set;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.NoSuchObjectException;

/**
 * Thread to poll nodes to see if they are still alive.
 */
public class AlivePoller extends Thread
{
    private GSNode parent;
    private ConnectionRegistry registry;
    
    public AlivePoller(GSNode parent, ConnectionRegistry registry)
    {
        this.parent = parent;
        this.registry = registry;
    }
    
    public void run()
    {
        while (true) {
            Set<String> rms = parent.getAllRMs();
            
            // Try to connect with each RM node to see if they're still alive
            // Note: only poll RM nodes, since we can detect GS nodes' deaths
            // soon enough simply by using the message passing system.
            for (String rm : rms) {
                if (!rm.substring(0, 2).equals("RM"))
                    continue;
                
                IRMNode rmNode = null;
                try {
                    registry.list();
                    rmNode = (IRMNode)registry.lookup(rm);
                    rmNode.alive();
                } catch (Exception e) {
                    parent.markRMNodeAsDead(rm);
                    
                    try {
                        UnicastRemoteObject.unexportObject(rmNode, true);
                    } catch (NoSuchObjectException nsoe) {
                        // Already unexported, so continue
                    }
                }
            }
            
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("AlivePoller got interrupted.");
            }
        }
    }
}
