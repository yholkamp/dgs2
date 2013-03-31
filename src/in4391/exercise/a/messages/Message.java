package in4391.exercise.a.messages;

public class Message extends AbstractMessage
{
    private String message;

    public Message(String message)
    {
        this.message = message;
    }

    public String getMessage()
    {
        return message;
    }

}