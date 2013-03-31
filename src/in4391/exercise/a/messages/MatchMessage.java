package in4391.exercise.a.messages;

/**
 * Message sent to indicate the matching of a RM with a GS node
 */
public class MatchMessage extends AbstractMessage
{
    private String gsNode;
    private String rmNode;
    
    public MatchMessage() {
        super(MessageParseType.GLOBAL);
    }
    
    public void setGSNode(String gsNode)
    {
        this.gsNode = gsNode;
    }
    
    public String getGSNode()
    {
        return gsNode;
    }
    
    public void setRMNode(String rmNode)
    {
        this.rmNode = rmNode;
    }
    
    public String getRMNode()
    {
        return rmNode;
    }
}
