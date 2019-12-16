package server;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

import domain.LogEntry;
import domain.LogEntry.Entry;
import enums.STATE;
import server.constants.Constants;
import server.service.IServerService;

/**
 * Para o leader comunicar com os followers
 */
public class FollowerCommunication extends Thread { 
	//dealy que vai ser para retentar comunicao
	private int delay; 
	private Registry r;
	private IServerService iServer;
	private int portF;
	private int verify;
	private Entry lastEntry;
	private boolean forElection;
	private LogEntry log;
	private Server server;
	private boolean readyForAnswer;


	public FollowerCommunication(Server server, int delay, boolean forElection, int port) { 
		this.portF = port;
		this.server = server;
		this.log = server.getLog();
		this.lastEntry = log.getLastEntry();

		connect();
		this.forElection = forElection;
		this.delay = delay; 

		System.out.println(portF + " --> " + this.getName());
	} 

	public void setElect(boolean ele) {
		forElection = ele;
	}

	@Override
	public void run(){ 
		try{ 
			if (!forElection) {
				while(server.getState().equals(STATE.LEADER)) {
					while(verify == 1) {
						Thread.sleep(delay); 
						connect();
					}

					Thread.sleep(1000);
					
					Entry e = log.getLastEntry();

					if( e == (null) || e.equals(lastEntry) ){
						verify = sendHeartBeat(iServer, null, 0);
					}else {
						int index;
						synchronized(server.getAnswers()) {
							index = server.getAnswers().size() - 1;
						}
						System.out.println("Veio entry " + e.toString());
						ArrayList <Entry> array = log.getLastEntriesSince(lastEntry);
						for (Entry entry : array) {

							verify = sendHeartBeat(iServer, entry.toString(), 0);
						}


						synchronized(server.getAnswers().get(index)) {


							server.addAnswers(index, verify);



							if(server.getnAnswers(index) < 2) {
								try {
									server.getAnswers().get(index).wait(8000);

								}catch (IllegalMonitorStateException i) {

								}
							}else {
								int count = 0;

								for (Integer integer : server.getAnswers().get(index)) {
									if(integer == 0)
										count ++;
								}
								if(count >= 2 && !e.isComitted()) {
									log.commitEntry(log.getCommitIndex() +1 );

								} 
								server.getAnswers().get(index).notifyAll();
							}

						}

						this.lastEntry = e;
					}
				}
			}
			else {
				if(verify != 1) {
					verify = sendHeartBeat(iServer, null, 1);

					synchronized(server.getVotes()) {
						server.addVote(portF, verify);

						server.incrementNAnswers();
						System.out.println(verify);

						if(server.getnAnswers() < 3) {
							System.out.println("Esperou : " + server.getnAnswers()) ;
							server.getVotes().wait(8000);

							server.setnAnswers(0);
						}else {
							System.out.println("Deu unlock");
							server.setnAnswers(0);
							server.getVotes().notifyAll();
							
							forElection = false;

						}
					}
				}
			}
		} 
		catch (InterruptedException | IllegalMonitorStateException e){ 
			return;
		} 
	}



	public void connect() {
		try {
			r = LocateRegistry.getRegistry(portF);
			iServer =  (IServerService) r.lookup(Constants.ADDRESS);
			verify = 0;
		}catch (RemoteException | NotBoundException e) {
			verify = 1;
		} 
	}

	/**
	 * Hearbeat enviado pelo leader. Envia AppendEntries vazio, se nao houver requests para enviar.
	 * 0 = correu tudo bem
	 * 1 = ta off
	 * 2 =  term < currentTerm
	 * 3 = log doesnt contain an entry at prevLogIndex whose term matches prevLogTerm
	 * @param serverI - stub do follower
	 * @param entry - entry a ser enviada
	 * @param election - 0 para enviar AppendEntriesRPC, 1 para enviar RequestVoteRPC
	 * @return
	 */
	public int sendHeartBeat(IServerService serverI, String entry, int election) {
		int flag;
		try {
			if(election == 0) {
				flag = serverI.AppendEntriesRPC(server.getTerm(), server.getPort(), log.getPrevLogIndex(),
						log.getPrevLogTerm(), entry, log.getCommitIndex());

				if(flag > 1) {
					server.setTerm(flag);
					server.setState(STATE.FOLLOWER);
				}
			}else if(election == 1 ) {
				System.out.println("vem aos votos : " + server.getPort());
				return serverI.RequestVoteRPC(server.getTerm(), server.getPort(), log.getPrevLogIndex(), log.getPrevLogTerm());
			}

		} catch (RemoteException e) {
			return 1;
		}
		return 0;

	}

	public boolean isReadyForAnswer() {
		return readyForAnswer;
	}

	public void setReadyForAnswer(boolean readyForAnswer) {
		this.readyForAnswer = readyForAnswer;
	}

} 