package in4391.exercise.a;

import in4391.exercise.a.util.ConnectionRegistry;
import in4391.exercise.a.core.IRMNode;
import in4391.exercise.a.wantds.Job;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * CLI application to run a GS node.
 */
public class RMIJobAdder
{
    static ConnectionRegistry registry;

    /**
     * Starts a new GS node, using the RMI registry at localhost or the host provided as first parameter
     *
     * @param args
     */
    public static void main(String args[])
    {
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
        String externalIp = (args.length < 1) ? null : args[0];

        try {
            registry = new ConnectionRegistry(LocateRegistry.getRegistry(externalIp), externalIp);
        } catch (RemoteException e) {
            e.printStackTrace();
            return;
        }

        int totalJobs = (args.length < 2) ? 1000 : Integer.valueOf(args[1]);
        // jobs of 10-60s are default
        int minDuration = (args.length < 3) ? 10000 : Integer.valueOf(args[2]);
        int maxDuration = (args.length < 4) ? 60000 : Integer.valueOf(args[3]);

        // ratio between the least and most loaded cluster
        int multiplier = 20;

        int jobId = 0;
        int lastPrint = totalJobs;

        Map<String, JobAddThread> threadMap = new HashMap<String, JobAddThread>();

        for (String id : registry.list()) {
            if (id.substring(0, 2).equals("RM")) {
                JobAddThread thread = new JobAddThread((IRMNode) registry.lookup(id));
                threadMap.put(id, thread);
                thread.start();
            }
        }

        while (totalJobs > 0) {
            // Print every ~50 jobs to prevent console output overload
            if (lastPrint > totalJobs + 50) {
                System.out.println("Adding jobs. " + totalJobs + " remaining.");
                lastPrint = totalJobs;
            }

            for (String id : threadMap.keySet()) {
                int i = 0;
                if (id.substring(0, 2).equals("RM")) {
                    int jobCount;
                    if (i == 0) {
                        jobCount = 1;
                    } else if (i == 1) {
                        jobCount = multiplier;
                    } else {
                        jobCount = (int) (1 + Math.random() * multiplier);
                    }

                    for (int jobI = 0; jobI < jobCount; jobI++) {
                        threadMap.get(id).addJob(new Job(jobId++, id, (int) (minDuration + Math.random() * (maxDuration - minDuration))));
                    }

                    i++;
                    totalJobs -= jobCount;
                }
            }

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.exit(0);
    }
}

class JobAddThread extends Thread
{

    private BlockingQueue<Job> jobQueue = new LinkedBlockingQueue<Job>();
    private IRMNode node;

    public JobAddThread(IRMNode node)
    {
        this.node = node;
    }

    public void run()
    {
        Job job;
        try {
            while ((job = jobQueue.take()) != null) {
                node.addJob(job);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (RemoteException e) {
            e.printStackTrace();
        }
    }

    public void addJob(Job job) {
        jobQueue.add(job);
    }
}
