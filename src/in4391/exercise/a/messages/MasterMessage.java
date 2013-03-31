package in4391.exercise.a.messages;

/**
 * Message identifying the sender as the current master node.
 */
public class MasterMessage extends AbstractMessage
{
    public MasterMessage() {
        super(MessageParseType.GLOBAL);
    }
}
