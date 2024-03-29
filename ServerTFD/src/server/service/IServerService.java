package server.service;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IServerService extends Remote {
	
	public String request(String s, int id) throws RemoteException;

	public int AppendEntriesRPC(int term, int leaderID, int prevLogIndex, int prevLogTerm,
			String entry, int leaderCommit) throws RemoteException;

	public int getPrevLogIndex() throws RemoteException;

	public int RequestVoteRPC(int term, int port, int prevLogIndex, int prevLogTerm) throws RemoteException;

	
}
