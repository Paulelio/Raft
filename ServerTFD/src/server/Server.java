package server;

import java.util.*;
import domain.LogEntry;
import enums.STATE;
import server.constants.Constants;

public class Server implements IServer {
	
	private int port;
	private int leaderPort;
	private STATE state;
	private int term;
	private int votedFor;
	
	private Timer timer;

	private ArrayList<String> pendentEntry = new ArrayList<>();
	private LogEntry log = new LogEntry();
	private Map<Integer, Integer> answers = new HashMap<>();

	private Map<Integer, Integer> votes = new HashMap<>();
	private FollowerCommunication first;
	private FollowerCommunication second;
	private FollowerCommunication third;
	private FollowerCommunication fourth;

	private List<FollowerCommunication> followers = Arrays.asList(first,second,third,fourth);

	private int nAnswers;

	public Server(int port){

		this.port = port;
		this.term = 0;
		this.state = STATE.FOLLOWER;
		this.nAnswers = 0;

		log.createFile(this.port);
	}

	/**
	 * Inicializa o server ---REVER---
	 */
	public void run() {
		Random r = new Random();
		//inicializar os servers de forma igual (retirar o que esta so no lider)

		int i = r.nextInt(3) + 1;

		timer = new Timer();
		RemindTask rt = new RemindTask(this);
		timer.schedule(rt, i * 10000);
	}

	/**
	 * Funcao que contem o bulk do trabalho realizado pelo Leader
	 */
	
	public void leaderWork() {

		ArrayList<Integer> ports = new ArrayList<>();

		for (Integer integer : Constants.PORTS_FOR_SERVER_REGISTRIES) {
			if(integer != this.port)
				ports.add(integer);
		}

		int j = 0;
		for (FollowerCommunication f : followers) {
			f = new FollowerCommunication(this,5000, false, ports.get(j));
			System.out.println("inicio canal de comunicao com o " + ports.get(j));
			f.start();
			j++;
		}

		while(true) {
			synchronized (answers) {
				if(answers.size() > 2){
					int count = 0;
					for (Integer i : answers.values()) {
						if(i == 0) 
							count ++;
					}
					if(count >= 2) {
						log.commitEntry();
					}

					answers = new HashMap<>();
				}
			}
		}
	}

	/**
	 * Funcao partilhada pelo ClientRMI, permite troca de mensagens com o client
	 * e o retorno de respostas.
	 * @return String de resposta, se for o leader, ou o porto do leader, se for um follower
	 */
	public String request(String s, int id)  {
		if(this.isLeader()) {
			synchronized(s){
				log.writeLog(s.split("_")[1] , this.term, false, s.split("_")[0] );
			}
			this.pendentEntry.add(s);
			return s.split("_")[1] ;
		}
		else {
			//devolve porto do leader
			return "& " + String.valueOf(leaderPort);
		}

	}

	

	/**
	 * Recebe AppendEntriesRPC do Leader
	 * @param term - termo do Leader
	 * @param leaderID - id do Leader (porto)
	 * @param prevLogIndex - prevLogIndex do Leader
	 * @param prevLogTerm - prevLogTerm do Leader
	 * @param entry - entry do Leader
	 * @param leaderCommit - ultimo commit do Leader
	 * @return true se tudo correu bem || false se ocorreu uma falha
	 */
	public int receiveAppendEntry(int term, int leaderID, int prevLogIndex, int prevLogTerm, String entry, int leaderCommit) {
		
		
		int ret = -1;
		if(term < this.getTerm()) {
			ret = this.getTerm();
		}else {
			timer.cancel();
			this.leaderPort = leaderID;
			if(entry == null) { 			
				ret = 0;
			}else {
				System.out.println(this.port);
				
				ret = log.writeLog(entry.split(":")[1] , this.term, false, entry.split(":")[4] ) ? 0 : 1 ;
				
			}
			resetTimer();
		}
		return ret;
	}

	/**
	 * Recebe RequestVoteRPC de um Candidate
	 * @param term - termo do candidato
	 * @param id - id do candidato (porto)
	 * @param prevLogIndex - prevLogIndex do candidato
	 * @param prevLogTerm - prevLogTerm do candidato
	 * @return -1 se ja votou, 0 se concorda com nova leadership, termo se nao concorda
	 */

	public int receiveRequestVote(int term, int id, int prevLogIndex, int prevLogTerm) {
		System.out.println("recebi voto : " + id);
		//ler paper para saber o que fazer aqui
		if (votedFor != 0) {
			System.out.println("nega voto");
			return -1;
		}
		else if (this.term > term) {
			//passa a candidato e inicia voto?
			return this.term;
		}
		else {
			System.out.println("olaaaa");
			votedFor = id;

			this.term = term;
			return 0;
		}
	}

	/**
	 * Faz reset a um timer (?) - devia ser static e receber um timer?
	 */
	public void resetTimer() {
		Random r = new Random();
		//inicializar os servers de forma ogual (retirar o que esta so no lider)

		int i = r.nextInt(3) + 1;
		
		timer = new Timer();
		timer.schedule(new RemindTask(this), i*10000);
	}
	

///// getter e setters 
	
	public void setVoteFor(int id) {
		this.votedFor = id;
	}

	public void cleanVote() {
		this.votedFor = 0;
	}
	
	public void addVote(int porto,int flag){
		votes.put(porto, flag);
	}
	
	public Map<Integer, Integer> getVotes(){
		return this.votes;
	}
	
	public void resetVotes(){
		this.votes = new HashMap<>();
	}

	public void setState(STATE state) {

		this.state = state;
	}

	public void increaseTerm() {
		this.term ++;
	}

	public int getPrevLogIndex() {
		return log.getPrevLogIndex();
	}

	public int getPort() {
		return this.port;
	}

	public int getLeaderPort() {
		return leaderPort;
	}
	public void setLeaderPort(int id){
		this.leaderPort = id;
	}

	public STATE getState() {
		return state;
	}

	public int getTerm() {
		return term;
	}

	public void setTerm(int term) {
		this.term = term;

	}

	public int getVotedFor() {
		return votedFor;
	}

	public boolean isLeader() {
		return this.state.equals(STATE.LEADER);
	}
	
	public void cancelTimer(){
		this.timer.cancel();
	}

	public int getnAnswers() {
		return nAnswers;
	}

	public void setnAnswers(int nAnswers) {
		this.nAnswers = nAnswers;
	}
	
	public void incrementNAnswers(){
		this.nAnswers++;
	}
	
	public LogEntry getLog(){
		return this.log;
	}
	
	public Map<Integer, Integer> getAnswers() {
		return answers;
	}

	public void addAnswers(int id, int flag) {
		this.answers.put(id, flag);
	}
	
	public List<FollowerCommunication> getFollowers() {
		return followers;
	}
	
	

}
