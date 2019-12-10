package domain;

import java.io.*;
import java.lang.Thread.State;
import java.util.*;

import javafx.util.Pair;
public class LogEntry {

	//Definir o que cada LogEntry vai ter
	// - num operacao
	// - id do client
	// - termo do server
	// - 
	private static final String BAR = System.getProperty("file.separator");

	private File f;
	private File s;
	private File dir;

	private int leaderID;
	private int prevLogIndex;
	private int prevLogTerm;
	private int commitIndex;
	private int port;

	private Entry lastEntry;

	private ArrayList<Entry> entries;
	private TableManager tManager;
	
	private List<Thread> followers;

	public LogEntry() {
		this.entries = new ArrayList<>();
		this.prevLogIndex = 0;
		this.commitIndex = -1;
		this.lastEntry = null;
		tManager = TableManager.getInstance();
	}

	public void createFile(int port) {
		this.port = port;
		String dire = "src" + BAR +"server" +BAR +"file_server_"+String.valueOf(port);
		String logFile = dire + BAR + "log_" + String.valueOf(port)+".txt";
		String snapshotFile = dire + BAR + "snapshot_" + String.valueOf(port)+".txt";
		// Use relative path for Unix systems


		this.dir = new File(dire);
		this.f = new File(logFile);
		this.s = new File(snapshotFile);
		if(!dir.exists()) {

			dir.mkdir();

			try {
				f.createNewFile();
				s.createNewFile();
			} catch (IOException e) {

				e.printStackTrace();
			}
		}else {
			loadEntries();
		}
	}

	private void loadEntries() {

		
		try {
			
			readFromSnapshot();
			
			readFromLog();
			
			
		} catch (NumberFormatException | IOException e) {

			e.printStackTrace();
		} 
	}
	
	private void readFromSnapshot() throws IOException {
		String st = ""; 
		
		//reads from snapshot
		FileInputStream fis = new FileInputStream(s);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		
		while ((st = br.readLine()) != null) {
			String[] stArr = st.split("-");
			tManager.putPair(stArr[0], stArr[0]);
		}
		br.close();
	}
	
	private void readFromLog() throws IOException {
		String st = ""; 
		
		//reads from snapshot
		FileInputStream fis = new FileInputStream(f);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));

		while ((st = br.readLine()) != null) {
			Entry e = Entry.setEntry(st);
			entries.add(e);
			if(e.isComitted()) {
				this.commitIndex ++;
			}
			String[] stArr = st.split(":")[1].split("-");
			
			if(stArr.length == 2) {//remove
				tManager.removePair(stArr[1]);
			}else 
				if(stArr.length == 3) {//put
					tManager.putPair(stArr[1], stArr[2]);
				}
			prevLogIndex ++;

		}
		br.close();
	}
	
	public boolean writeLog(String command, int term, boolean commited, String id_command) {

		Entry aux = new Entry(prevLogIndex,command,term,commited,id_command);
		if(entries.contains(aux)) {
			return false;
		}else {
			lastEntry =  aux;
			
			synchronized (entries) {
				entries.add(lastEntry);
			}

			prevLogIndex ++;
			
			for (Thread t : followers) {
				if(t.getState() == State.TIMED_WAITING) t.interrupt();
			}
			
			try {
				if(f.length() > 300) {
					generateSnapshot();
					clearLogFile();
				}
				
				BufferedWriter writer = new BufferedWriter(new FileWriter(f,true));
				writer.write(lastEntry.toString());
				writer.newLine();
				writer.close();

				return true;

			} catch (IOException e) {
				return false;
			}
		}
	}

	private void generateSnapshot() throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(s,true));
		
		List<Pair<String,String>> table = tManager.getList();
		
		for (Pair<String, String> pair : table) {
			writer.write(pair.getKey() + "-" + pair.getValue());
			writer.newLine();
		}
		
		writer.close();
	}

	private void clearLogFile() throws IOException {
		synchronized (f) { //locks log file
			
			String dire = "src" + BAR +"server" +BAR +"file_server_"+String.valueOf(port);
			String logFileCopy = dire + BAR + "log_" + String.valueOf(port)+".txt"; //file path, n sei se tem de ser diferente
			
			File logCopy = new File(logFileCopy);
			
			BufferedWriter writer = new BufferedWriter(new FileWriter(logCopy,false));
		
			//Assumindo que o que estah no ficheiro log = array entries
			
			for (Entry entry : entries) {
				if(!entry.isComitted()) { //se ja tiver committed, ja foi para o snapshot, logo pode-se remover
					writer.write(entry.toString());
				}
			}
			
			//overwrites the old file for the new one
			f.delete();
			f = logCopy;
			
			f.createNewFile();
			
		}
	}

	public static class Entry{
		private int index;
		private String command;
		private int term;
		private boolean commited;
		private String clientIDCommand;


		public Entry(int index, String command, int term, boolean commited, String id_command) {
			this.index = index;
			this.command = command;
			this.term = term;
			this.commited = commited;
			this.clientIDCommand = id_command;
		}

		public String toString() {
			return (String.valueOf(index)+":"+command+":"+String.valueOf(term)+":"+
					Boolean.toString(commited)+":"+String.valueOf(clientIDCommand));
		}

		public static Entry setEntry(String s) {
			String[] array = s.split(":");

			return new Entry(Integer.parseInt(array[0]),array[1],Integer.parseInt(array[2]),Boolean.valueOf(array[3]),
					(array[4]));
		}

		public boolean isComitted() {
			return commited;
		}

		public String getClientIDCommand() {
			return clientIDCommand;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Entry other = (Entry) obj;
			if (clientIDCommand == null) {
				if (other.clientIDCommand != null)
					return false;
			} else if (!clientIDCommand.equals(other.clientIDCommand))
				return false;
			if (command == null) {
				if (other.command != null)
					return false;
			} else if (!command.equals(other.command))
				return false;
			if (commited != other.commited)
				return false;
			if (index != other.index)
				return false;
			if (term != other.term)
				return false;
			return true;
		}




		public void setCommitted() {
			this.commited = true;

			// temos que ir ao log mudar isto pah!!!!!!
		}

	}
	
	//TEMOS DE FAZER ISTO lmao
	public void commitEntry() { 
		//TODO
		commitIndex ++;

		System.out.println("ESTA COMITADO");
	}

	public int getPrevLogTerm() {
		return prevLogTerm;
	}

	public void setPrevLogTerm(int prevLogTerm) {
		this.prevLogTerm = prevLogTerm;
	}

	public int getPrevLogIndex() {
		return prevLogIndex;
	}

	public void setPrevLogIndex(int prevLogIndex) {
		this.prevLogIndex = prevLogIndex;
	}

	public int getCommitIndex() {
		return commitIndex;
	}

	public void setCommitIndex(int commitIndex) {
		this.commitIndex = commitIndex;
	}

	public Entry getLastEntry() {
		return lastEntry;
	}

	public ArrayList<Entry> getLastEntriesSince(Entry e) {

		ArrayList <Entry> array = new ArrayList<>();
		int flag = 0;
		if(e == null) {
			flag = 1;
		}
		for(Entry entry : entries) {

			if(flag == 1) {
				array.add(entry);
			}
			if((entry.equals(e))&& flag != 1) {
				flag = 1;
			}
		}
		return array;
	}

	public String getEntry(String string) {
		for (Entry entry : entries) {
			if(entry.command.split("-")[1].compareTo(string) == 0) {
				return entry.command.split("-")[2];
			}
		}
		return null;
		
	}
	
	public void setFollowerThreads(List<Thread> f) {
		this.followers = f;
	}
}
