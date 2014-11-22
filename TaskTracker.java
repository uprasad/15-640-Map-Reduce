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
		
		// the first argument is the IP address of the JobTracker
		jobTrackerIP = args[0];
		
		// the second argument is the port number of the JobTracker
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
			
			// Error in registering with the JobTracker
			if (pollPort == null) {
				System.out.println("TaskTracker has not been recognized. Shame on your family.");
				System.exit(1);
			}
			
			// Get the listening port (servPort) and the unique node number of this TaskTracker
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
		
		/*
		 * Handles the command to copy a file into the TaskTracker's node
		 * 
		 * The TaskTracker reads the name of the file to be read.
		 * It then proceeds to read the file 1KB at a time, and writes
		 * it onto disk.
		 */
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
			    
			    // reads the file 1KB at a time, and writes it to disk
			    while((bytesRead = is.read(bytearray)) > 0 ) {
			    	bos.write(bytearray, 0, bytesRead);
			    }
			    bos.close();
			    
			    //oos.writeObject("OK");
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		/*
		 * Handles the command to run a Map task
		 * 
		 * The TaskTracker reads other details from the JobTracker
		 * like the partition number of the input file it has to work on, 
		 * the input directory and the unique ID of the job. It then constructs
		 * an output directory string and a Map task command, and spawns a new
		 * thread to run the Map task using the Map.class file.
		 */
		else if (command.equals("RunMap")) {
			Integer partition = null;
			Integer jobId = null;
			Integer numReducers = null;
			String inputDir = null;
			try {
				partition = (Integer)ois.readObject();
				jobId = (Integer)ois.readObject();
				numReducers = (Integer)ois.readObject();
				inputDir = (String)ois.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// construct the output directory
			String outputDir = "./root/" + Integer.toString(nodeNumber) + 
					"/" + inputDir + "_" + Integer.toString(jobId) + 
					"_" + Integer.toString(partition) + "out";
			
			// construct the input directory
			inputDir = "./root/" + Integer.toString(nodeNumber) + 
					"/" + inputDir + Integer.toString(partition);
			
			// construct the directory where the Map.class file is
			String mapDir = "root/" + Integer.toString(nodeNumber) + 
					"/job" + Integer.toString(jobId);
			
			// construct the map command
			String mapCommand = "java -cp " + mapDir + "/ " + 
					"Map " + inputDir + " " + outputDir;
			
			// finally, run the map task on a new thread
			RunProcess mapProcess = new RunProcess(mapCommand, jobId, partition, numReducers, true);
			Thread t = new Thread(mapProcess);
			t.start();
			
			// send the JobTracker acknowledgement that the task has been started
			try {
				oos.writeObject("MapDone"); // TODO find out when to say MapDone
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		/*
		 * Handles the command to run a Reduce task
		 * 
		 * The TaskTracker takes a few more inputs like the unique ID
		 * for the job, the input and output directories. Then, similar to the
		 * Map phase of the job, it constructs an input directory, an output
		 * directory and a reduce command. Finally, the TaskTracker proceeds to
		 * run the reduce task in a new thread.
		 */
		else if (command.equals("RunReduce")) {
			Integer jobId = null;
			Integer numMappers = null;
			Integer reduceNum = null;
			String inputDir = null;
			String outputDir = null;
			
			// read the necessary details from the JobTracker
			try {
				jobId = (Integer)ois.readObject();
				numMappers = (Integer)ois.readObject();
				reduceNum = (Integer)ois.readObject();
				inputDir = (String)ois.readObject();
				outputDir = (String)ois.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// construct the input directory path
			inputDir = "./root/" + Integer.toString(nodeNumber) + 
					"/" + inputDir + "_" + Integer.toString(jobId) + 
					"red" + Integer.toString(reduceNum);
			
			// construct the directory where the input is
			String reduceDir = "root/" + Integer.toString(nodeNumber) + 
					"/job" + Integer.toString(jobId);
			
			// construct the output directory
			outputDir = "./root/" + Integer.toString(nodeNumber) + "/" + 
					outputDir + Integer.toString(reduceNum);
			
			System.out.println("************" + inputDir);
			
			// construct the reduce command
			String reduceCommand = "java -cp " + reduceDir + "/ " + 
					"Reduce " + inputDir + " " + outputDir;
			System.out.println("************" + outputDir);
			
			// finally, start the reduce task
			RunProcess reduceProcess = new RunProcess(reduceCommand, jobId, reduceNum, 0, false);
			Thread t = new Thread(reduceProcess);
			t.start();
			
			// send a message to the JobTracker to acknowledge successful
			// start of the reduce task
			try {
				oos.writeObject("ReduceDone"); // TODO find out when to say MapDone
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}

/*
 * The RunProcess class contains the method needed to start and run
 * a new process by passing a command string as input
 */
class RunProcess implements Runnable {
	
	// the command to be run
	String command = null;
	
	// the unique job ID
	int jobId = 0;
	
	// the partition of the input on which the command is run
	int partition = 0;
	
	// the number of reducers
	int numReducers = 0;
	
	// a true/false value to distinguish between the map and reduce phases
	boolean isMap;
	
	/*
	 * METHOD: combiner
	 * INPUT: a file object and the number of reducers
	 * OUTPUT: void
	 * 
	 * The method is for post-processing after the map phase. The
	 * output of the map phase is all combined into "numReducers" number
	 * of files, by using a hash function on the key values in the map 
	 * output modulo numReducers.
	 */
	void combiner(File file, int numReducers) {
		
		// create as many files as the number of reducers
		File[] outFiles = new File[numReducers];
		FileOutputStream[] outFos = new FileOutputStream[numReducers];
		BufferedWriter[] outBw = new BufferedWriter[numReducers];
		
		// initialize the file streams
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
		
		/*
		 * get each key from the map output file, and use the hashCode()
		 * method modulo the number of reducers and write it out into 
		 * the file indexed by that value
		 */
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
		
		/*
		 * After the combiner is done, we have numReducers files,
		 * each of which contains a part of the output of the map phase.
		 * 
		 * In the reduce phase, each reducer pulls that part of the file which
		 * is indexed by the reducer number. The combiner stage just prepares
		 * the output of the map task to be pulled by the reducers 
		 */
		
		System.out.println("Combiner done");
	}
	
	/* the constructor for the class */
	RunProcess(String command, int jobId, int partition, int numReducers, boolean isMap) {
		this.command = command;
		this.jobId = jobId;
		this.partition = partition;
		this.numReducers = numReducers;
		this.isMap = isMap;
	}
	
	/* printing the output for debugging */
	private static void printLines(String name, InputStream ins) throws Exception {
	    String line = null;
	    BufferedReader in = new BufferedReader(
	        new InputStreamReader(ins));
	    while ((line = in.readLine()) != null) {
	        System.out.println(name + " " + line);
	    }
	}
	
	/*
	 * METHOD: runProcess
	 * INPUT: the command that is to be run
	 * OUTPUT: void
	 * 
	 * The runProcess method takes a String that is the command to be
	 * executed. Once it executes the program, depending on if it is a 
	 * Map or a Reduce task, it sends an appropriate result.
	 */
	void runProcess(String command) { 
		try {
			System.out.println(command);
			
			// Execute the command given by the "command" String
			Process pro = Runtime.getRuntime().exec(command);
			
			// print the debugging info
			printLines(command + " stdout:", pro.getErrorStream());
			pro.waitFor();
			System.out.println(command + " exitValue() " + pro.exitValue());
			
			// If the task is a map task
			if(this.isMap) {
				if (pro.exitValue() == 0) {
					String fileName = command.split(" ")[command.split(" ").length - 1];
					File file = new File(fileName);
					combiner(file, numReducers);
				}
				
				sendMapResult(pro.exitValue());
			}
			// if the task is a reduce task
			else {
				sendReduceResult(pro.exitValue());
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * Sends the result of the map task to the JobTracker
	 */
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
	
	/*
	 * Sends the result of the reduce task to the jobtracker
	 */
	private void sendReduceResult(int exitValue) {
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
			
			oos.writeObject("ReduceResult");
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
/*
 * This class runs a loop that listens to incoming poll
 * requests from the PollClient class in the JobTracker
 */
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