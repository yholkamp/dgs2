package in4391.exercise.a.messages;

/**
 * Message sent to indicate the arrival of a new GS node
 */
public class GSLeaveMessage extends AbstractMessage
{
    private String name;
    
    public GSLeaveMessage() {
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
