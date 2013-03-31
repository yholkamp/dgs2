package in4391.exercise.a.messages;

/**
 *
 */
public class MasterElectionMessage extends AbstractMessage
{
    private String deadMaster;
    
    public MasterElectionMessage() {
        super(MessageParseType.GLOBAL);
    }
    
    public String getDeadMaster()
    {
        return deadMaster;
    }

    public void setDeadMaster(String deadMaster)
    {
        this.deadMaster = deadMaster;
    }
}
