package in4391.exercise.a.messages;

public enum MessageParseType {
    DIRECTED,   // Messages specific for one GS, thus ignored by everyone else
    GLOBAL,     // Messages of (often) global importance, parsed if the receiver is null
    MASTER, SYSTEM      // Messages regarding Jobs and their events, stored by every GS.
}
