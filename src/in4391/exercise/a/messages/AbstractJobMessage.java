package in4391.exercise.a.messages;

import in4391.exercise.a.wantds.Job;

/**
 *
 */
public abstract class AbstractJobMessage extends AbstractMessage
{
    private Job job;

    public AbstractJobMessage(MessageParseType type)
    {
        super(type);
    }

    public AbstractJobMessage()
    {
        super(MessageParseType.SYSTEM);
    }

    public Job getJob()
    {
        return job;
    }

    public void setJob(Job job)
    {
        this.job = job;
    }

    @Override
    public String toString()
    {
        return "{" + "from: " + sender + ", to: " + receiver + ", t: " + timestamp + ", class: " + getClass().getSimpleName() + ", job: " + getJob() + " }";
    }

}
