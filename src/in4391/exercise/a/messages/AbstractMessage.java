package in4391.exercise.a.messages;

import java.io.Serializable;

public abstract class AbstractMessage implements Serializable, Cloneable
{
    public final MessageParseType PARSE_TYPE;
    int timestamp = -1;
    String sender;
    String receiver;

    public AbstractMessage()
    {
        PARSE_TYPE = MessageParseType.DIRECTED;
    }

    public AbstractMessage(MessageParseType type)
    {
        PARSE_TYPE = type;
    }

    public int getTimestamp()
    {
        return timestamp;
    }

    public void setTimestamp(int timestamp)
    {
        this.timestamp = timestamp;
    }

    public String getSender()
    {
        return sender;
    }

    public void setSender(String sender)
    {
        this.sender = sender;
    }

    public String getReceiver()
    {
        return receiver;
    }

    public void setReceiver(String receiver)
    {
        this.receiver = receiver;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException
    {
        return super.clone();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractMessage that = (AbstractMessage) o;

        if (!sender.equals(that.sender)) return false;
        if (timestamp != that.timestamp) return false;

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = timestamp;
        result = 31 * result + sender.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "{" + "from: " + sender + ", to: " + receiver + ", t: " + timestamp + ", class: " + getClass().getSimpleName() + "}";
    }
}


