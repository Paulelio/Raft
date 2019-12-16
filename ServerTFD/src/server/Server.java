package server;

import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;

import javax.naming.spi.ResolveResult;

import domain.LogEntry;
import domain.TableManager;
import enums.STATE;
import server.constants.Constants;

public class Server implements IServer {

	private int port;

	private int leaderPort;
	private STATE state;
	private int term;
	private int votedFor;

	private boolean readyToAnswer;
	private Timer timer;


	private ArrayList<String> pendentEntry = new ArrayList<>();
	private LogEntry log = new LogEntry();
	//private Map<Integer, Pair<Integer,Integer>> answers = new HashMap<>();

	private List<List<Integer>> answers = new ArrayList<>();
	private Map<Integer, Integer> votes = new HashMap<>();
	private FollowerCommunication first;
	private FollowerCommunication second;
	private FollowerCommunication third;
	private FollowerCommunication fourth;

	private String result = null;

	private List<FollowerCommunication> followers = Arrays.asList(first,second,third,fourth);

	private TableManager tManager;

	private int nAnswers;

	public Server(int port){

		this.port = port;
		this.term = 0;
		this.state = STATE.FOLLOWER;
		this.nAnswers = 0;
		this.tManager = TableManager.getInstance();
		log.createFile(this.port);
		int i = 0;
		while(i < log.getEntriesSize()) {
			answers.add(new ArrayList<>());
			i ++;
		}
		this.readyToAnswer = false;
	} 

	/**
	 * Inicializa o server ---REVER---
	 */
	public void run() {
		Random r = new Random();
		//inicializar os servers de forma igual (retirar o que esta so no lider)

		int i = r.nextInt(2) + 1;
		int e = r.nextInt(2) + 1;
		timer = new Timer();
		RemindTask rt = new RemindTask(this);
		timer.schedule(rt, i * e * 10000);
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
			System.out.println("Inicio canal de comunicao com o " + ports.get(j));
			f.start();
			j++;
		}

		log.setFollowerThreads(followers);



	}

	/**
	 * Funcao partilhada pelo ClientRMI, permite troca de mensagens com o client
	 * e o retorno de respostas.
	 * @return String de resposta, se for o leader, ou o porto do leader, se for um follower
	 */
	public String request(String s, int id)  {
		if(this.isLeader()) {
			answers.add(new ArrayList<>());

			result = null;




			int cIndex = log.getCommitIndex();
			//uniqueID | id _ operation
			String aux = s.split(":")[0];
			//operation
			String operation = aux.split("_")[1];
			//operation : values
			String object = s.split("_")[1];
			//
			String ss = s.split("_")[0];


			if(operation.compareTo("l") == 0) {
				return tManager.toString();
			}

			if(operation.compareTo("g") == 0) {
				String value = tManager.getValue(object.split(":")[1]);
				return value instanceof String ? value : "Nao existe";
			}

			if(operation.compareTo("c") == 0) {
				String value = tManager.getValue(object.split(":")[1]);
				return value instanceof String ? value : "Nao existe";
			}
			
			this.pendentEntry.add(s);
			result = tableManager(operation, object,ss) == 0 ? "Sucesso!" : "Falhou!";
			
			
			return result;
		}

		else {
			//devolve porto do leader
			return "& " + String.valueOf(leaderPort);
		}

	}



	private int tableManager(String operation, String object, String s) {
		switch (operation) {

		case "p":
			return log.writeLog(operation +"-" + object.split(":")[1]+"-"+object.split(":")[2], this.term, false, s ) ? 0 : 1;	

		case "d":
			return log.writeLog(operation +"-" + object.split(":")[1], this.term, false, s ) ? 0 : 1;
			
		case "c":
			return log.writeLog(operation +"-" + object.split(":")[1] +"-"+object.split(":")[2] +"-"+object.split(":")[3], term, false, s) ? 0 : 1;

		default:
			return -1;
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

		System.out.println("Recebi heartbeat " + term +" | " + leaderID);
		int ret = -1;
		if(term < this.getTerm()) {
			ret = this.getTerm();

		}else {
			this.state = STATE.FOLLOWER;
			timer.cancel();
			this.leaderPort = leaderID;
			if(leaderPort == votedFor) votedFor = 0;

//			if(leaderCommit > log.getCommitIndex()) {
//				
//				log.commitEntry(leaderCommit);
//			}

			if(entry == null) { 
				ret = 0;
			}else {
				
				String operation = entry.split(":")[1].split("-")[0];

				String object = entry.split(":")[1].replace("-", ":");

				String s = entry.split(":")[4];

				ret = tableManager(operation, object, s);
				if(operation.compareTo("g") == 0)
					if(ret != -1)
						ret = 0;
				
				System.out.println("Follower return " + ret);

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

		if(this.term < term) {
			votedFor = id;
			this.term = term;
			this.state = STATE.FOLLOWER;
			resetTimer();

			return 0;
		}

		else if(prevLogTerm == log.getPrevLogTerm() && prevLogIndex > log.getPrevLogIndex()) {
			return this.term;
		}


		else if (votedFor != 0) {

			System.out.println("nega voto");
			resetTimer();
			return -1;
		}
		else if (this.term > term) {

			return this.term;
		}

		else {
			votedFor = id;
			this.term = term;
			resetTimer();
			return 0;
		}
	}

	/**
	 * Faz reset a um timer (?) - devia ser static e receber um timer?
	 */
	public void resetTimer() {
		Random r = new Random(port);
		//inicializar os servers de forma ogual (retirar o que esta so no lider)

		int i = r.nextInt(3) + 1;
		int e = r.nextInt(2) + 1;
		timer.cancel();
		timer = new Timer();
		timer.schedule(new RemindTask(this), i*10000 + e*1000);
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

	public int getnAnswers(int index) {
		return answers.get(index).size();
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

	public List<List<Integer>> getAnswers() {
		return answers;
	}

	public void addAnswers(int index,int flag) {
		this.answers.get(index).add(flag);
	}

	public List<FollowerCommunication> getFollowers() {
		return followers;
	}

	public int addNewEntry() {
		answers.add(new ArrayList<Integer>());
		return answers.size()-1;
	}

	public void answerReady(boolean b) {
		readyToAnswer = b;

	}

	public boolean getAnReady() {
		return readyToAnswer;
	}




}
