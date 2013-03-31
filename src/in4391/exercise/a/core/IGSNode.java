package in4391.exercise.a.core;

import java.rmi.RemoteException;

/**
 * Remote interface for the GSNodes
 */
public interface IGSNode extends INode
{
    public int calculateLoad() throws RemoteException;

}
