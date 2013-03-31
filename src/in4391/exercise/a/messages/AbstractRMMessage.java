package in4391.exercise.a.messages;

/**
 * Base class for messages sent to an RM.
 */
public class AbstractRMMessage extends AbstractMessage
{
    public AbstractRMMessage()
    {
        super(MessageParseType.SYSTEM);
    }
}
