package in4391.exercise.a.messages;

/**
 * Message sent to indicate the arrival of a new GS node
 */
public class GSJoinMessage extends AbstractMessage
{
    public GSJoinMessage() {
        super(MessageParseType.MASTER);
    }
}