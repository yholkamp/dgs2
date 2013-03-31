package in4391.exercise.a.messages;

import in4391.exercise.a.wantds.Job;

import java.util.*;
import java.util.concurrent.*;

/**
 * Message containing the current system log, used to update new GS nodes
 */
public class LogStateMessage extends AbstractMessage
{
    private Queue<AbstractMessage> log;
    private ConcurrentMap<String, Set<String>> matchedRMs;
    private ConcurrentMap<String, Queue<Job>> jobQueues;


    public Queue<AbstractMessage> getLog()
    {
        return log;
    }

    public void setLog(Queue log)
    {
        this.log = log;
    }

    public void setMatchedRMs(ConcurrentMap<String, Set<String>> matchedRMs)
    {
        this.matchedRMs = matchedRMs;
    }

    public ConcurrentMap<String, Set<String>> getMatchedRMs()
    {
        return matchedRMs;
    }

    public void setJobQueues(ConcurrentMap<String, Queue<Job>> jobQueues)
    {
        this.jobQueues = jobQueues;
    }

    public ConcurrentMap<String, Queue<Job>> getJobQueues()
    {
        return jobQueues;
    }
}
