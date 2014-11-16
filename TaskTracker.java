import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

public class TaskTracker implements Runnable {
	//JobTracker details
	static String jobTrackerIP = null;
	static int jobTrackerPort;
	
	//polling and server ports
	static Integer pollPort = null;
	static Integer servPort = null;
	
	//node number in cluster
	static Integer nodeNumber = null;
	
	//connection for thread
	static Socket newConnection = null;
	
	public TaskTracker(Socket connection) {
		newConnection = connection;
	}
	
	public static void main(String args[]) {
		if (args.length < 2) {
			System.out.println("Usage: java TaskTracker <JobTracker IP> <JobTracker port>");
			System.exit(1);
		}
		
		jobTrackerIP = args[0];
		jobTrackerPort = Integer.parseInt(args[1]);
		
		/* Connect to JobTracker*/
		Socket connection = null;
		ObjectOutputStream oos = null;
		ObjectInputStream ois = null;
		try {
			connection = new Socket(jobTrackerIP, jobTrackerPort);
			oos = new ObjectOutputStream(connection.getOutputStream());
			oos.flush();
			ois = new ObjectInputStream(connection.getInputStream());
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		/*Send request for identification*/
		try {
			oos.writeObject("NewTaskTracker");

			/*Read polling port given by JobTracker*/
			pollPort = (Integer)ois.readObject();
			
			if (pollPort == null) {
				System.out.println("TaskTracker has not been recognized. Shame on your family.");
				System.exit(1);
			}
			
			servPort = (Integer)ois.readObject();
			nodeNumber = (Integer)ois.readObject();
			
			System.out.println("This is the Tasktracker's nodeNumber: " + nodeNumber);
			System.out.println("This is the TaskTracker's servPort: " + servPort);
			System.out.println("This is the TaskTracker's pollPort: " + pollPort);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/*Start listening for poll requests by JobTracker*/
		Runnable pollRunnable = new PollServer();
		Thread pollThread = new Thread(pollRunnable);
		pollThread.start();
		
		/*Listen to requests on servPort*/
		ServerSocket taskTrackerSocket = null;
		
		try {
			taskTrackerSocket = new ServerSocket(servPort);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/*Wait for accept() on servPort*/
		while(true) {
			Socket servConnection = null;
			
			/*Accept new connections*/
			try {
				servConnection = taskTrackerSocket.accept();
				System.out.println("Request accepted");
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			Thread taskTrackerThread = null;

			/*Instantiate a new Thread for listening to connections*/		
			try {
				TaskTracker taskTrackerRunnable = new TaskTracker(servConnection);
				taskTrackerThread = new Thread(taskTrackerRunnable);
				taskTrackerThread.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			System.out.println("TaskTracker has accepted a request.");
		}
	}
	
	//Thread for handling requests
	public void run() {
		ObjectInputStream ois = null;
		ObjectOutputStream oos = null;
		
		try {
			oos = new ObjectOutputStream(newConnection.getOutputStream());
			oos.flush();
			ois = new ObjectInputStream(newConnection.getInputStream());
			System.out.println("TaskTracker streams made.");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		String command = null;
		
		/* Read Request*/
		try {
			command = (String)ois.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if (command.equals("CopyFile")) {
			String fileName = null;
			try {
				//copy file to node
				fileName = (String)ois.readObject();
				
				byte[] bytearray = new byte[1024];
				
				//file reader stream
				InputStream is = newConnection.getInputStream();
				
				//file writer stream
				FileOutputStream fos = new FileOutputStream(fileName);
			    BufferedOutputStream bos = new BufferedOutputStream(fos);
			    
			    int bytesRead;
			    
			    while((bytesRead = is.read(bytearray)) > 0 ) {
			    	bos.write(bytearray, 0, bytesRead);
			    }
			    bos.close();
			    
			    //oos.writeObject("OK");
			} catch(Exception e) {
				e.printStackTrace();
			}
		} else if (command.equals("RunMap")) {
			Integer partition = null;
			Integer jobId = null;
			String inputDir = null;
			try {
				partition = (Integer)ois.readObject();
				jobId = (Integer)ois.readObject();
				inputDir = (String)ois.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			String outputDir = "./root/" + Integer.toString(nodeNumber) + 
					"/" + inputDir + "_" + Integer.toString(jobId) + 
					"_" + Integer.toString(partition) + "out";
			
			inputDir = "./root/" + Integer.toString(nodeNumber) + 
					"/" + inputDir + Integer.toString(partition);
			
			String mapDir = "root/" + Integer.toString(nodeNumber) + 
					"/job" + Integer.toString(jobId);
			String mapCommand = "java -cp " + mapDir + "/ " + 
					"Map " + inputDir + " " + outputDir;
			RunProcess mapProcess = new RunProcess(mapCommand, jobId, partition, true);
			Thread t = new Thread(mapProcess);
			t.start();
			
			try {
				oos.writeObject("MapDone"); // TODO find out when to say MapDone
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else if (command.equals("RunReduce")) {
			Integer jobId = null;
			Integer numMappers = null;
			Integer reduceNum = null;
			String inputDir = null;
			try {
				jobId = (Integer)ois.readObject();
				numMappers = (Integer)ois.readObject();
				reduceNum = (Integer)ois.readObject();
				inputDir = (String)ois.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			inputDir = "./root/" + Integer.toString(nodeNumber) + 
					"/" + inputDir + "_" + Integer.toString(jobId) + 
					"red" + Integer.toString(reduceNum);
			
			String reduceDir = "root/" + Integer.toString(nodeNumber) + 
					"/job" + Integer.toString(jobId);
			System.out.println("************" + inputDir);
			String reduceCommand = "java -cp " + reduceDir + "/ " + 
					"Reduce " + inputDir + " " + inputDir + "out";
			RunProcess reduceProcess = new RunProcess(reduceCommand, jobId, reduceNum, false);
			Thread t = new Thread(reduceProcess);
			t.start();
			
			try {
				oos.writeObject("ReduceDone"); // TODO find out when to say MapDone
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}

class RunProcess implements Runnable {
	String command = null;
	int jobId = 0;
	int partition = 0;
	boolean isMap;
	
	void combiner(File file, int numReducers) {
		File[] outFiles = new File[numReducers];
		FileOutputStream[] outFos = new FileOutputStream[numReducers];
		BufferedWriter[] outBw = new BufferedWriter[numReducers];
		
		try {
			for (int i=0; i<numReducers; i++) {
				outFiles[i] = new File(file.getAbsolutePath() + Integer.toString(i+1));
				outFos[i] = new FileOutputStream(outFiles[i]);
				outBw[i] = new BufferedWriter(new OutputStreamWriter(outFos[i]));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			String line;
			String key;
			while ((line = br.readLine()) != null) {
				key = line.split("\t")[0];
				int red = Math.abs(key.hashCode())%numReducers;
				outBw[red].write(line);
				outBw[red].newLine();
			}
			
			for (int i=0; i<numReducers; i++) {
				outBw[i].close();
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("Combiner done");
	}
	
	RunProcess(String command, int jobId, int partition, boolean isMap) {
		this.command = command;
		this.jobId = jobId;
		this.partition = partition;
		this.isMap = isMap;
	}
	
	private static void printLines(String name, InputStream ins) throws Exception {
	    String line = null;
	    BufferedReader in = new BufferedReader(
	        new InputStreamReader(ins));
	    while ((line = in.readLine()) != null) {
	        System.out.println(name + " " + line);
	    }
	}
	
	void runProcess(String command) { 
		try {
			System.out.println(command);
			Process pro = Runtime.getRuntime().exec(command);
			printLines(command + " stdout:", pro.getErrorStream());
			pro.waitFor();
			System.out.println(command + " exitValue() " + pro.exitValue());
			
			if(this.isMap) {
				if (pro.exitValue() == 0) {
					int numReducers = 3; // TODO GET REDUCERS FROM CONFIG FILE
					String fileName = command.split(" ")[command.split(" ").length - 1];
					File file = new File(fileName);
					combiner(file, numReducers);
				}
				
				sendMapResult(pro.exitValue());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void sendMapResult(int exitValue) {
		Socket connection = null;
		ObjectOutputStream oos = null;
		ObjectInputStream ois = null;
		int jobTrackerPort = TaskTracker.jobTrackerPort;
		String jobTrackerIP = TaskTracker.jobTrackerIP;
		try {
			connection = new Socket(jobTrackerIP, jobTrackerPort);
			oos = new ObjectOutputStream(connection.getOutputStream());
			oos.flush();
			ois = new ObjectInputStream(connection.getInputStream());
			
			oos.writeObject("MapResult");
			oos.writeObject(jobId);
			oos.writeObject(partition);
			oos.writeObject(exitValue);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public void run() {
		runProcess(command);
	}
}

class PollServer implements Runnable {
	public void run() {
		int port = TaskTracker.pollPort;
		
		ServerSocket servSock = null;
		ObjectOutputStream oos = null;
		ObjectInputStream ois = null;
		String command = null;
		try {
			servSock = new ServerSocket(port);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/* Accept polling requests from JobTracker*/
		while(true) {
			Socket connection = null;
			try {
				connection = servSock.accept();
				oos = new ObjectOutputStream(connection.getOutputStream());
				oos.flush();
				ois = new ObjectInputStream(connection.getInputStream());
				command = (String)ois.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (command.equals("Poll")) {
				try {
					/*Reply to poll*/
					oos.writeObject("Alive");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			/* close() socket after replying back to poll*/
			try {
				connection.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}