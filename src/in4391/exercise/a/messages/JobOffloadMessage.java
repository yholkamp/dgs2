package in4391.exercise.a.messages;

import in4391.exercise.a.wantds.Job;

/**
 * Message sent to indicate a job has been offloaded to a certain GS node.
 */
public class JobOffloadMessage extends AbstractJobMessage
{
    public JobOffloadMessage(Job job) {
        super(MessageParseType.SYSTEM);
        this.setJob(job);
    }
}
