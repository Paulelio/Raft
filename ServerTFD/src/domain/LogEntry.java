package domain;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.List;

import javafx.util.Pair;
import server.FollowerCommunication;
public class LogEntry {

	//Definir o que cada LogEntry vai ter
	// - num operacao
	// - id do client
	// - termo do server
	// - 
	private static final String BAR = System.getProperty("file.separator");

	private static final int MAX_FILE_SIZE_BYTES = 150;

	private File f;
	private File s;
	private File dir;


	private int prevLogIndex;
	private int prevLogTerm;
	private int commitIndex;
	private int port;
	private int snapshotEntries;
	private Entry lastEntry;

	private ArrayList<Entry> entries;
	private TableManager tManager;

	private List<FollowerCommunication> followers;

	public LogEntry() {
		this.entries = new ArrayList<>();
		this.snapshotEntries = 0;
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
			snapshotEntries ++;
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

				String[] stArr = st.split(":")[1].split("-");

				if(stArr.length == 2) //remove
					tManager.removePair(stArr[1]);

				if(stArr.length == 3) {//put
					tManager.putPair(stArr[1], stArr[2]);
				}
				if(stArr.length == 4) {//put
					tManager.putPair(stArr[1], stArr[3]);

				}
			}
			prevLogIndex = e.getIndex() +1;
		}
		if(prevLogIndex == 0 && snapshotEntries > 0 )
			prevLogIndex = snapshotEntries;
		br.close();
	}

	public boolean writeLog(String command, int term, boolean commited, String id_command) {

		Entry aux = new Entry(prevLogIndex,command,term,commited,id_command);
		for (Entry entry : entries) {
			if(entry.getClientIDCommand().compareTo(aux.getClientIDCommand()) == 0)
				return false;
		}
		lastEntry =  aux;

		synchronized (entries) {
			entries.add(lastEntry);
			commitEntry(entries.size() -1);
		}

		prevLogIndex ++;
		prevLogTerm = lastEntry.term;

		if(followers instanceof List<?>) {
			for (Thread t : followers) {
				if(t instanceof FollowerCommunication)
					if(t.getState() == State.TIMED_WAITING) 
						t.interrupt(); //supostamente acorda quando ha algo novo
			}
		}

		try {
			//			BufferedWriter writer = new BufferedWriter(new FileWriter(f,true));
			//			writer.write(lastEntry.toString());
			//			writer.newLine();
			//			writer.close();

			if(f.length() > MAX_FILE_SIZE_BYTES) {
				generateSnapshot();
				clearLogFile();
			}

			return true;

		} catch (IOException e) {
			return false;
		}

	}

	private void generateSnapshot() throws IOException {


		BufferedWriter writer = new BufferedWriter(new FileWriter(s,false));

		List<Pair<String,String>> table = tManager.getList();

		for (Pair<String, String> pair : table) {
			writer.write(pair.getKey() + "-" + pair.getValue());
			writer.newLine();

		}

		for (Entry entry : entries) {
			if (entry.isComitted()) {
				entry.setISnap();
			}
		}

		writer.close();
	}

	private void clearLogFile() throws IOException {
		synchronized (f) { //locks log file

			FileWriter memes = new FileWriter(f,false);
			memes.write("");
			memes.close();


			BufferedWriter writer = new BufferedWriter(new FileWriter(f,true));

			//Assumindo que o que estah no ficheiro log = array entries
			for (Entry entry : entries) {

				if(!entry.isComitted()) { //se ja tiver committed, ja foi para o snapshot, logo pode-se remover
					System.out.println("to commit" + entry);
					writer.write(entry.toString());
					writer.newLine();
				}
			}


			writer.close();
		}
	}

	public static class Entry{
		private int index;
		private String command;
		private int term;
		private boolean commited;
		private String clientIDCommand;
		private boolean inSnap;


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

		public int getIndex() {
			return index;
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
		}

		public void setISnap() {
			this.inSnap = true;
		}

		public boolean getISnap() {
			return this.inSnap;
		}
	}


	public void commitEntry(int i) { 

		String dire = "src" + BAR +"server" +BAR +"file_server_"+String.valueOf(port);
		String logFile = dire + BAR + "log_" + String.valueOf(port)+".txt";
		System.out.println(i + " memes " + port);
		String [] arra = entries.get(i).toString().split(":")[1].split("-");

		synchronized (f) {
			f.delete();


			f = new File(logFile);
			try {
				//Escreve entries jah committed que nao deviam ir para o log
				//mete enties nao-committed como committed e não as executa na tabela
				BufferedWriter writer = new BufferedWriter(new FileWriter(f,true));
				int j = 0;
				for (Entry entry : entries) {
					if (!entry.getISnap()) {
						if(j <= i && !entry.commited) {
							entry.setCommitted();
							switch (arra[0]) {
							case "p":
								tManager.putPair(arra[1], arra[2]);
								break;
							case "d":
								tManager.removePair(arra[1]);
								break;

							case "c":
								tManager.cas(arra[1], arra[2], arra[3]);
								break;
							}
						}
						writer.write(entry.toString());
						System.out.println(entry.toString());
						writer.newLine();
					}

					j ++;
				}
				writer.close();

				f.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		commitIndex++;

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

	public void setFollowerThreads(List<FollowerCommunication> followers2) {
		this.followers = followers2;
	}

	public int getEntriesSize() {
		return entries.size();
	}
}
