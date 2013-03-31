package in4391.exercise.a.wantds;

import java.io.Serializable;

/**
 * A single job.
 */
public class Job implements Serializable
{
    private static final long serialVersionUID = 7526471155622776148L;

    private int id;
    private String clusterId;
    private int duration;
    private JobStatus status = JobStatus.Waiting;

    public Job(int id, String clusterId, int duration)
    {
        this.id = id;
        this.clusterId = clusterId;
        this.duration = duration;
    }

    public int getId()
    {
        return id;
    }

    public String getClusterId()
    {
        return clusterId;
    }

    public int getDuration()
    {
        return duration;
    }

    public JobStatus getStatus()
    {
        return status;
    }

    public void setStatus(JobStatus status)
    {
        this.status = status;
    }

    @Override
    public String toString()
    {
        return "Job {id: " + id + ", clusterId: " + clusterId + ", status: " + status + "}";
    }

    @Override
    public int hashCode()
    {
        return id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Job job = (Job) o;

        if (id != job.id) return false;

        return true;
    }
}
