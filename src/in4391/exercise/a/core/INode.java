package in4391.exercise.a.core;

import in4391.exercise.a.messages.AbstractMessage;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface INode extends Remote
{
    void receive(AbstractMessage message) throws RemoteException;
    boolean alive() throws RemoteException;

    int[] getNodeState() throws RemoteException;
}
