package DTO;

import java.io.*;
import java.util.*;

public class LogEntry {

	//Definir o que cada LogEntry vai ter
	// - num operacao
	// - id do client
	// - termo do server
	// - 
	private static final String BAR = System.getProperty("file.separator");
	
	private File f;
	private File dir;
	
	private int leaderID;
	private int prevLogIndex;
	private int prevLogTerm;
	private int commitIndex;
	
	private Entry lastEntry;
	
	private Map<String,Entry> entries;
	
	
	public LogEntry() {
		this.entries = new HashMap<>();
		this.prevLogIndex = 0;
		this.commitIndex = 0;
		this.lastEntry = null;
	}
	
	public void createFile(int port) {
		String dire = "src" + BAR +"server" +BAR +"file_server_"+String.valueOf(port);
		String file = dire + BAR + "log_" + String.valueOf(port)+".txt";
		// Use relative path for Unix systems

		System.out.println(file);
		this.dir = new File(dire);
		this.f = new File(file);
		if(!dir.exists()) {
			
			dir.mkdir();



			try {
				f.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else {
			loadEntries();
		}
	}

	private void loadEntries() {


		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(f));
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 

		String st = ""; 
		try {
			while ((st = br.readLine()) != null) {
				Entry e = Entry.setEntry(st);
				entries.put(st.split(":")[4], e);
				
				if(e.isComitted()) {
					this.commitIndex ++;
				}
				
				prevLogIndex ++;
			}
		} catch (NumberFormatException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}
	
	
	public boolean writeLog(String command, int term, boolean commited, String id_command) {

		if(entries.get(id_command) != null) {
			
		}else {
		
		lastEntry =  new Entry(prevLogIndex,command,term,commited,id_command);

		entries.put(id_command,lastEntry);
		prevLogIndex ++;


		try {

			BufferedWriter writer = new BufferedWriter(new FileWriter(f,true));
			writer.write(lastEntry.toString());
			writer.newLine();
			writer.close();
			
			return true;
			
		} catch (IOException e) {
			return false;
		}

		}
		return false;
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
					Boolean.toString(commited)+":"+String.valueOf(clientIDCommand)+"\n");
		}

		public static Entry setEntry(String s) {
			String[] array = s.split(":");

			return new Entry(Integer.parseInt(array[0]),array[1],Integer.parseInt(array[2]),Boolean.valueOf(array[3]),
					(array[4]));
		}
		
		public boolean isComitted() {
			return commited;
		}

	}


	public void commitEntry() {
	
		
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
	
}
