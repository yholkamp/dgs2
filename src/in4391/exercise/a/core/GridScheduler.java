package in4391.exercise.a.core;

import java.util.*;
import in4391.exercise.a.messages.JobAssignMessage;
import in4391.exercise.a.wantds.Cluster;
import in4391.exercise.a.wantds.Job;

/**
 * Thread for the actual scheduling.
 */
public class GridScheduler extends Thread
{
    private GSNode gsNode;
    private boolean running = true;
    private long pollSleep = 1000;
    private Map<String, Integer> loadEstimates;
    
    public GridScheduler(GSNode gsNode)
    {
        this.gsNode = gsNode;
    }
    
    /**
     * Select the RM with the least load.
     */
    private String getLeastLoadedRM()
    {
        int minLoad = Integer.MAX_VALUE;
        String minRM = null;
        for (String rm : loadEstimates.keySet()) {
            int load = loadEstimates.get(rm);
            if (load < minLoad) {
                minLoad = load;
                minRM = rm;
            }
        }
        
        // Don't schedule if the minimum load is higher than just 25% space left in the job queues
        if (minRM == null) {
            System.out.println("Scheduler: no RMs found");
            return null;
        } else if (minLoad > (Cluster.SIZE + RMNode.MAX_QUEUE_SIZE) * 0.75) {
            System.out.println("Scheduler: all RMs overcapacity (minLoad: " + minLoad + ")");
            return null;
        } else {
            return minRM;
        }
    }
    
    /**
     * Take a job and assign it to the least loaded RM.
     */
    private boolean scheduleJob(Job job)
    {
        String rm = getLeastLoadedRM();
        
        System.out.println("RM: " + rm);
        
        boolean scheduled = false;
        if (rm != null) {
            JobAssignMessage message = new JobAssignMessage();
            message.setSender(gsNode.getId());
            message.setReceiver(rm);
            message.setJob(job);
            GSMessageChannel ch = gsNode.getOrCreateChannel(rm);
            
            // Send to the RM node, as well as all GS nodes
            ch.sendMessage(message);
            gsNode.send(message);
            
            scheduled = true;
            
            // Update our load estimate now that the RM got a new job
            int newLoad = loadEstimates.get(rm) + 1;
            loadEstimates.put(rm, newLoad);
        } else {
            System.out.println("ScheduleJob: Found no RM with capacity");
        }
        
        return scheduled;
    }
    
    public void run()
    {
        while (running) {
            Queue<Job> jobQueue = gsNode.getJobQueue();
            // System.out.println("Scheduling... (jobs: " + jobQueue.size() + ", RMs: " + gsNode.getMatchedRMs().size() + ")");
            
            // Cache the loads, so we can update them as "guesses"
            // before the actual loads change
            loadEstimates = new HashMap<String, Integer>();
            for (String rm : gsNode.getMatchedRMs()) {
                loadEstimates.put(rm, gsNode.getLoadForRM(rm));
            }
            
            // Schedule all jobs in the current queue
            // Note that the queue may be added to in the process, so we
            // explicitly iterate up to size()
            // Need to make sure that we never remove(E) in another thread!
            int length = jobQueue.size();
            for (int i = 0; i < length; i++) {
                Job job = jobQueue.peek();
                boolean scheduled = scheduleJob(job);
                if (scheduled) {
                    jobQueue.remove();
                } else {
                    // Can't schedule right now
                    break;
                }
            }
            
            try {
                Thread.sleep(pollSleep);
            } catch (InterruptedException ex) {
                System.out.println("Scheduler " + gsNode.getId() + " was interrupted.");
            }
        }
    }
    
    /**
     * Stop the polling thread. This has to be called explicitly to make sure the program 
     * terminates cleanly.
     */
    public void stopThread() {
        running = false;
        try {
            this.join();
        } catch (InterruptedException ex) {
            System.out.println("Scheduler " + gsNode.getId() + " was interrupted.");
        }
    }
}
