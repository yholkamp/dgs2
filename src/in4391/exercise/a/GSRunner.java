package in4391.exercise.a;

import in4391.exercise.a.core.GSNode;
import in4391.exercise.a.wantds.Job;
import in4391.exercise.a.util.ConnectionRegistry;
import in4391.exercise.a.messages.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

/**
 * CLI application to run a GS node.
 */
public class GSRunner
{
    protected GSNode gsNode = null;
    private String rmiHost = null;

    /**
     * Starts a new GS node, using the RMI registry at localhost or the host provided as first parameter
     *
     * @param args
     */
    public static void main(String args[])
    {
        System.out.println("Press <NodeId><Enter> to send a message (type 'q' to exit):");

        GSRunner gsRunner = new GSRunner();
        String externalIp = (args.length < 1) ? null : args[0];
        gsRunner.setRMIHost(externalIp);
        gsRunner.startProcess();

        // Set up console reading
        String line;
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

        Registry registry;
        try {
            registry = LocateRegistry.getRegistry(externalIp);
        } catch (RemoteException e) {
            e.printStackTrace();
            return;
        }

        // Wait for user input
        while (true) {
            try {
                line = in.readLine();
                if (line == null || line.equals("q"))
                    break; // Quit
                else if (line.equals("c")) {
                    gsRunner.gsNode.printChannels();
                    continue;
                } else if (line.equals("r")) {
                    System.out.println("Current registry:");
                    for (String binding : registry.list()) {
                        System.out.println(binding);
                    }
                    continue;
                } else if (line.equals("j")) {
                    System.out.println("Send job:");
                    Job job = new Job(1, "RMNode00", 10);
                    JobOffloadMessage jobMessage = new JobOffloadMessage(job);
                    jobMessage.setSender(gsRunner.gsNode.getId());
                    jobMessage.setReceiver("GSNode02");
                    gsRunner.gsNode.send(jobMessage);
                    continue;
                } else if (line.equals("m")) {
                    System.out.println("Master: " + gsRunner.gsNode.getMasterName());
                    continue;
                } else if (line.equals("p")) {
                    // print the system log
                    gsRunner.gsNode.writeLog();
                    continue;
                } else if (line.equals("?")) {
                    System.out.println("Available commands:");
                    System.out.println("c       - print the open message channels");
                    System.out.println("r       - prints the current registry bindings");
                    System.out.println("j       - sends an offloaded job to the GSNode");
                    System.out.println("m       - print the name of the master node");
                    System.out.println("p       - print the system log");
                    System.out.println("[0-9]   - sends a message to the node identified by GSNode<number>");
                    System.out.println("q       - exit");
                    continue;
                }

                // Broadcast message to all known processes
                Message message = new Message(String.format("%s says hello!", gsRunner.gsNode.getId()));
                message.setSender(gsRunner.gsNode.getId());
                message.setReceiver(String.format("GSNode%02d", Integer.valueOf(line)));
                gsRunner.gsNode.send(message);
            } catch (IOException e) {
                System.err.println("Server exception: " + e.toString());
                e.printStackTrace();
            } catch (NumberFormatException e) {
                System.out.println("Invalid number provided");
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Continuing");
            }
        }

        gsRunner.gsNode.stop();

        // Explicitly quit, since RMI is still running
        System.exit(0);
    }

    public GSRunner()
    {
        setUpShutdownHook();
    }

    private int getNewProcessId(ConnectionRegistry registry) throws RemoteException
    {
        // Get all registered processes
        List<String> bindings = registry.list();

        // This is the first gsNode
        if (bindings.size() == 0)
            return 1;

        System.out.println("Looking for binding");

        // Find the last GSNode id
        int lastBinding = 0;
        for (String binding : bindings) {
            if (binding.substring(0, 2).equals("GS")) {
                int num = Integer.valueOf(binding.split("GSNode")[1]);
                if (num > lastBinding)
                    lastBinding = num;
            }
        }

        return lastBinding + 1;
    }

    public void startProcess()
    {
        try {
            // Locate the registry and determine new gsNode id
            Registry registry = LocateRegistry.getRegistry(rmiHost);
            int pid = getNewProcessId(new ConnectionRegistry(registry, rmiHost));
            gsNode = new GSNode(String.format("GSNode%02d", pid), registry, rmiHost);
            gsNode.init();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void setRMIHost(String host)
    {
        this.rmiHost = host;
    }

    public void stopNode()
    {
        if (gsNode == null)
            return;

        try {
            // Locate the registry and deregister the process
            Registry registry = LocateRegistry.getRegistry(rmiHost);
            registry.unbind(gsNode.getId());
            UnicastRemoteObject.unexportObject(gsNode, true);

            // Process terminated
            System.out.println(String.format("%s shutdown", gsNode.getId()));

            gsNode = null;
        } catch (Exception e) {
            System.err.println("Server exception: " + e.toString());
            e.printStackTrace();
        }
    }

    public void setUpShutdownHook()
    {
        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            public void run()
            {
                System.out.println("Shutting down...");

                // Stop process
                stopNode();
            }
        });
    }

}
