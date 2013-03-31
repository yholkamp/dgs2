package in4391.exercise.a.messages;

/**
 *
 */
public class GSDeadMessage extends AbstractMessage
{
    private String deadNode;

    public GSDeadMessage() {
        super(MessageParseType.SYSTEM);
    }

    public String getDeadNode()
    {
        return deadNode;
    }

    public void setDeadNode(String deadNode)
    {
        this.deadNode = deadNode;
    }
}
