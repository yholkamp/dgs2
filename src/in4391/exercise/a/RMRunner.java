package in4391.exercise.a;

import in4391.exercise.a.wantds.Job;
import in4391.exercise.a.core.RMNode;
import in4391.exercise.a.util.ConnectionRegistry;

import java.util.List;
import java.util.Random;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 * CLI application to run an RM node.
 */
public class RMRunner
{
    protected RMNode rmNode = null;
    private String rmiHost = null;
    private JobAdder jobAdder;
    private int jobFrequency;

    public RMRunner()
    {
        setUpShutdownHook();
    }

    /**
     * Starts a new RM node, using the RMI registry at localhost or the host provided as first parameter
     *
     * @param args
     */
    public static void main(String args[])
    {
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }

        RMRunner rmRunner = new RMRunner();
        String rmiHost = (args.length < 1) ? null : args[0];
        rmRunner.setRMIHost(rmiHost);
        rmRunner.setJobFrequency(args.length < 2 ? 0 : Integer.valueOf(args[1]));
        rmRunner.startProcess();

        // Set up console reading
        String line;
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

        Registry registry;
        try {
            registry = LocateRegistry.getRegistry(rmiHost);
        } catch (RemoteException e) {
            e.printStackTrace();
            return;
        }

        // Wait for user input
        while (true) {
            try {
                Thread.sleep(100);
                line = in.readLine();
                if (line == null || line.equals("q")) {
                    break; // Quit
                } else if (line.equals("r")) {
                    System.out.println("Current registry:");
                    for (String binding : registry.list()) {
                        System.out.println(binding);
                    }
                    continue;
                } else if (line.equals("j")) {
                    System.out.println("Job queue has " + rmRunner.rmNode.getJobQueue().size() + " entries:");

                    for (Job j : rmRunner.rmNode.getJobQueue()) {
                        System.out.println(j.toString());
                    }

                    continue;
                }

                System.out.println("Adding a new job to this RMNode");
                Job job = new Job((int) (10000 * Math.random()), rmRunner.rmNode.getId(), 30000);
                rmRunner.rmNode.addJob(job);
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

        rmRunner.rmNode.stop();

        // Explicitly quit, since RMI is still running
        System.exit(0);
    }

    private int getNewProcessId(ConnectionRegistry registry) throws RemoteException
    {
        // Get all registered processes
        List<String> bindings = registry.list();

        // This is the first rmNode
        if (bindings.size() == 0)
            return 1;

        // Find the last rmNode id
        int lastBinding = 0;
        for (String binding : bindings) {
            if (binding.substring(0, 2).equals("RM")) {
                int num = Integer.valueOf(binding.split("RMNode")[1]);
                if (num > lastBinding)
                    lastBinding = num;
            }
        }

        return lastBinding + 1;
    }

    public void startProcess()
    {
        try {
            // Locate the registry and determine new rmNode id
            Registry registry = LocateRegistry.getRegistry(rmiHost);
            int pid = (new Random()).nextInt(1000000);//getNewProcessId(new ConnectionRegistry(registry, rmiHost));
            rmNode = new RMNode(String.format("RMNode%08d", pid), registry, rmiHost);
            rmNode.init();

            jobAdder = new JobAdder(rmNode, jobFrequency);
            jobAdder.start();
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
        if (rmNode == null)
            return;

        try {
            // Locate the registry and deregister the process
            Registry registry = LocateRegistry.getRegistry(rmiHost);
            registry.unbind(rmNode.getId());
            UnicastRemoteObject.unexportObject(rmNode, true);

            // Process terminated
            System.out.println(String.format("%s shutdown", rmNode.getId()));

            rmNode = null;
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

    public void setJobFrequency(int jobFrequency)
    {
        this.jobFrequency = jobFrequency;
    }

}

/**
 * Simple thread adding jobs to the system at a predefined rate.
 */
class JobAdder extends Thread
{
    int frequency = 1;
    RMNode rmNode;

    public JobAdder(RMNode rmNode, int frequency)
    {
        this.rmNode = rmNode;
        this.frequency = frequency;
    }

    public void run()
    {
        if (frequency == 0)
            return;

        while (true) {
            try {
                Thread.sleep(1000 / frequency);

                Job job = new Job((int) (100000 * Math.random()), rmNode.getId(), (int) (10000 * Math.random()));
                rmNode.addJob(job);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
