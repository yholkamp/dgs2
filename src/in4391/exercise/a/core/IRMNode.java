package in4391.exercise.a.core;

import in4391.exercise.a.wantds.Job;

import java.rmi.RemoteException;

/**
 * Remote interface for the RMNodes
 */
public interface IRMNode extends INode
{
    public void addJob(Job job) throws RemoteException;
}
