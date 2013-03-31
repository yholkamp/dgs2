package in4391.exercise.a.messages;

/**
 * Message sent to indicate the arrival of a new RM
 */
public class RMJoinMessage extends AbstractMessage
{
    public RMJoinMessage() {
        super(MessageParseType.SYSTEM);
    }
}
