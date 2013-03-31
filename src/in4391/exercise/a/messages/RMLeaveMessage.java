package in4391.exercise.a.messages;

/**
 * Message sent to indicate the leaving of an RM
 */
public class RMLeaveMessage extends AbstractMessage
{
    private String name;
    
    public RMLeaveMessage() {
        super(MessageParseType.SYSTEM);
    }
    
    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }
}
