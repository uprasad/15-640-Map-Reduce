import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;
import java.util.jar.*;

public class JobTracker implements Runnable {

	//newConnection for every thread
	protected Socket newConnection = null;
	
	//seedPort for Polling
	static int seedPort = 9000;
	
	//taskSeedPort is TaskTracker's servPort
	static int taskSeedPort = 12000;
	
	// job ID
	static int jobId = 0;
	
	//unique nodeNum for TaskTracker(s)
	static int nodeNum = 1;
	
	//maintain connection for TaskTracker and TaskTrakerInfo
	static Hashtable<Socket, TaskTrackerInfo> taskTrackerTable = new Hashtable<Socket, TaskTrackerInfo>();
	//FileSystem for DFS
	static FileSystem fileSystem = null;
	
	/* Table of jobs indexed by jobId
	 * 1. hashed by jobId
	 * 2. Job object contains details of Map & Reduce tasks of particular Job
	 */
	static Hashtable<Integer, Job> jobTable = new Hashtable<Integer, Job>();
	
	/*
 	 * Constructor for thread
 	 */ 
	public JobTracker(Socket connection) {
		newConnection = connection;
	}
	
	public static void main(String args[]) {
		
		if (args.length < 1) {
			System.out.println("Usage: java JobTracker <port>");
			System.exit(1);
		}
		
		// The first argument should be the port number of the Job Tracker server
		int jobTrackerPort = Integer.parseInt(args[0]);
		
		String jobTrackerIP = null;
		// Get Job Tracker IP address
		try {
			jobTrackerIP = InetAddress.getLocalHost().getHostAddress().toString();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// Initialize File System
		fileSystem = new FileSystem(jobTrackerIP, jobTrackerPort);
		
		/*
		 * Create a new ServerSocket to listen to incoming connections 
		 */
		ServerSocket jobTrackerSocket = null;
		try {
			jobTrackerSocket = new ServerSocket(jobTrackerPort);
		} catch (Exception e) {
			e.printStackTrace();
		}

		/* Poll data nodes in a separate thread for status*/
		Runnable pollRunnable = new PollClient();
		Thread pollThread = new Thread(pollRunnable);
		pollThread.start();
		
		/* Read requests from MapReduce and TaskTracker(s)*/
		while(true) {
			Socket connection = null;
			
			/*Accept new connections*/
			try {
				connection = jobTrackerSocket.accept();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			Thread jobTrackerThread = null;

			/*Start a new Thread for each accepted connection*/		
			try {
				JobTracker jobTrackerRunnable = new JobTracker(connection);
				jobTrackerThread = new Thread(jobTrackerRunnable);
				jobTrackerThread.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			System.out.println("JobTracker has accepted a request");
		}
	}
	
	/* Thread for handling requests from MapReduce and TaskTracker(s)*/
	public void run() {
		//Initialize input and output streams for the connection
		ObjectInputStream ois = null;
		ObjectOutputStream oos = null;
		
		try {
			oos = new ObjectOutputStream(newConnection.getOutputStream());
			oos.flush();
			ois = new ObjectInputStream(newConnection.getInputStream());
			System.out.println("JobTracker streams made");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		String command = null;
		
		/* Read Request */
		try {
			command = (String)ois.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/*
		 * Handle the connection of a new Task Tracker
		 * 
		 * This block of code stores the details of the new connection
		 * in a TaskTrackerInfo object and adds this object to the
		 * taskTrackerTable, a table of all the task trackers in
		 * the cluster.
		 * 
		 * Additionally, the JobTracker also sends the unique ID,
		 * the polling port (we check whether the TaskTrackers are alive
		 * by polling.
		 */
		if (command.equals("NewTaskTracker")) {
			//add TaskTracker to the taskTrackerTable
			TaskTrackerInfo taskTrackerInfo = new TaskTrackerInfo(newConnection.getInetAddress().getHostAddress(), seedPort, taskSeedPort, nodeNum);
			taskTrackerTable.put(newConnection, taskTrackerInfo);
			
			try {
				oos.writeObject(seedPort); //polling port
				oos.writeObject(taskSeedPort); //servPort for TaskTracker
				oos.writeObject(nodeNum); //unique nodeNum
				
				seedPort++;
				taskSeedPort++;
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			System.out.println("JobTracker has added a new TaskTracker " + 
					newConnection.getInetAddress().toString());

			// add node to FileSystem (refer to method definition for details)
			fileSystem.addNode(taskTrackerInfo);

			nodeNum++;
		}
		/*
		 * Handle the request for a list of the Task Trackers
		 * 
		 * When the application programmer requests a list of the
		 * Task Trackers, traverse the Task Tracker table and return the
		 * Task Tracker Info in list form
		 */
		else if (command.equals("ListTaskTrackers")) {
			/* send a List TaskTracker(s) to MapReduce*/
			ArrayList<TaskTrackerInfo> taskTrackerInfo = 
					new ArrayList<TaskTrackerInfo>(taskTrackerTable.values());
			
			try {
				oos.writeObject(taskTrackerInfo);
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("JobTracker replied to ListTaskTracker.");
		}
		/*
		 * Handle request for information about a job
		 * 
		 * Given a job ID, this block of code returns the list of
		 * mappers and reducers running on the clusters at that point 
		 * in time.
		 */
		else if (command.equals("JobInfo")) {
			Integer jobId = null;
			try {
				jobId = (Integer)ois.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			Job jobInfo = jobTable.get(jobId);
			
			try {
				oos.writeObject(jobInfo);
				oos.writeObject(jobInfo.mapList);
				oos.writeObject(jobInfo.reduceList);
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("JobTracker replied to JobInfo.");
		}
		/*
		 * Handle the request for the output files path in the DFS
		 * 
		 * This block of code returns the relative paths of the 
		 * output files, once the Map-Reduce job is done.
		 */
		else if (command.equals("GetOutput")) {
			String outputDir = null;
			try {
				outputDir = (String)ois.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			//check if outputDir in FileSystem 
			outputDir = fileSystem.getFormattedFileName(outputDir);
			int present = fileSystem.filePresent(outputDir);
			
			FileEntry outEntry = null;
			
			if(present == 1) {
				outEntry = fileSystem.getFileEntry(outputDir);
			}
			
			try {
				oos.writeObject(outEntry);
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("JobTracker replied to GetOutput.");
		}
		/*
		 * Handle the request to copy data into the DFS
		 * 
		 * This block of code takes a source directory in the local file
		 * system as input and copies the contents of that directory into the DFS,
		 * so that it can later be accessed by any of the Task Trackers.
		 */
		else if (command.equals("copy")) {
			/* copy data into DFS*/
			
			//Source Directory src
			String src = null;
			try {
				src = (String)ois.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			//add to FileSystem
			int addStatus = fileSystem.addFile(src);

			try {
				/*ADD checks if dest dir exists in DFS here*/
				if (addStatus == 1) {
					oos.writeObject("notFound");
					System.out.println("Source directory " + src + "not found");	
				} else if (addStatus == 2) {
					oos.writeObject("notDir");
				} else if (addStatus == 3){					
					oos.writeObject("duplicate");
				} else if (addStatus == 0){
					System.out.println(src + " successfully copied.");
					oos.writeObject("OK");
				}
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
		/*
		 * ?
		 */
		else if (command.equals("copyFile")) {
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
		}
		/*
		 * Handle a request to delete a folder from the DFS
		 * 
		 * This block of code takes as input a directory name, checks if
		 * that directory exists in the DFS, and deletes it.
		 * 
		 * This command MUST be sent to the JobTracker if any directory is
		 * to be overwritten i.e. the directory must first be deleted, and
		 * then the new directory with the same name copied in later.
		 */
		else if (command.equals("delete")) {
			/* delete data from DFS*/
			
			//Source Directory src
			String src = null;
			try {
				src = (String)ois.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			//add to FileSystem
			int deleteStatus = fileSystem.deleteFile(src);

			try {
				/*ADD check if dest dir exists in DFS here*/
				if (deleteStatus == 1) {
					oos.writeObject("notFound");
					System.out.println("Source directory " + src + " not found");	
				} else if (deleteStatus == 0){
					System.out.println(src + " successfully deleted");
					oos.writeObject("OK");
				}
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
		/*
		 * Handles the request to start a new job on the cluster
		 * 
		 * The JobTracker assigns a unique ID to the requested job, reads
		 * the other details about the job from the application programmer
		 * directly or using the configuration files.
		 * 
		 * The JobTracker then extracts the input into a special directory.
		 * It then schedules and starts the Map phase, followed by the
		 * scheduling and running of the Reduce phase. 
		 */
		else if (command.equals("NewJob")) {
			
			jobId++;
			String inputDir = null;
			String outputDir = null;
			Integer numReducers = null;
			
			try {
				inputDir = (String)ois.readObject();
				outputDir = (String)ois.readObject();
				numReducers = (Integer)ois.readObject();
			} catch (Exception e) { 
				e.printStackTrace();
			}
			
			//check if inputDir exists
			boolean valid = false;
			try {
				if (fileSystem.filePresent(inputDir) == 0) {
					oos.writeObject("NotExist");
				} else {
					valid = true;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			//check if outputDir is not duplicate
			try {
				outputDir = fileSystem.getFormattedFileName(outputDir);
				if (fileSystem.filePresent(outputDir) == 1) {
					oos.writeObject("OutDuplicate");
					valid = false;
				} else {
					int status = fileSystem.addOutputFile(outputDir, numReducers);
					if(status == 1) {
						oos.writeObject("OutDuplicate");
						valid = false;
					}
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
			
			// send AckDir to acknowledge validity of input and output directories
			if(valid) {
				try {
					oos.writeObject("AckDir");
				} catch(Exception e) {
					e.printStackTrace();
				}
				System.out.println("Input directory for new job: " + inputDir);
			}
			
			if (valid) { // If the job is valid
				int numMappers = fileSystem.getNumParts(inputDir);
				// Extract the .jar containing the Mapper and Reducer programs
				extractJAR(jobId);
				
				// create new job and add to jobTable
				Job newJob = new Job(numMappers, numReducers, jobId, inputDir, outputDir);
				jobTable.put(jobId, newJob);
				
				String iDir = jobTable.get(jobId).getInputDir();
				System.out.println("new jobID: " + jobId + " inputDir: " + iDir);
				
				String destDir = "./root";
				String jarFolder = destDir + File.separator + "mapred" + jobId;
				
				// scheduler for mapper stage
				int[] mapperToNode = mapperScheduler(inputDir, numMappers, jobId);
				//fileSystem.splitToPartitions(inputDir, numMappers, jobId);
				
				int inputDirLength = fileSystem.getFileLength(inputDir);
				
				// loop over mappers, start them
				for (int i=0; i<numMappers; i++) { 
					// add the job to the list of Map tasks
					jobTable.get(jobId).mapList.add(i, new TaskDetails(i+1, mapperToNode[i], 
							(double)inputDirLength/numMappers));
					
					// as the filesystem to move the input partitions to the 
					// respective nodes
					fileSystem.sendPartitionToNode(inputDir, i+1, mapperToNode[i], jobId);
					
					File mapFile = new File(jarFolder + File.separator + "Map.class");
					String dirName = "job" + Integer.toString(jobId);
					
					// send the Map.class file to the particular TaskTracker node's folder
					fileSystem.sendToNodeFolder(mapperToNode[i], mapFile, dirName);
					
					// Now that the input partitions and the Map.class files
					// are in the TaskTracker's execution environment, 
					// start the map task
					startMapTask(jobId, i+1, mapperToNode[i], inputDir);
				}
			}
		}
		/*
		 * This message is sent by each mapper when a map phase is completed.
		 * 
		 * The JobTracker takes in the job ID of the job, the partition on which
		 * the map task has been completed, and the exit value of the map task.
		 * 
		 * It then checks the exit value to see if there were any errors in the 
		 * task. In case of errors, it informs the user of the exit value. Otherwise,
		 * it increments the count for the number of completed map tasks.
		 * 
		 * Once the number of completed map tasks reaches the total number of mappers
		 * for the particular job, it starts the Reduce phase of the job.
		 */
		else if (command.equals("MapResult")) {
			Integer jobId = null;
			Integer partition = null;
			Integer exitValue = null;
			// read details of the completed map task
			try {
				jobId = (Integer)ois.readObject();
				partition = (Integer)ois.readObject();
				exitValue = (Integer)ois.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// if map task was not successful
			if (exitValue != 0) {
				System.out.println("Map task " + partition + " of jobId " + jobId + " failed.");
				
				//update jobList
				jobTable.get(jobId).mapList.get(partition-1).setStatus(3);
				int nodeNum = jobTable.get(jobId).mapList.get(partition-1).getNodeNum();
				
				// update TaskTracker load
				// load = size of the inputs on the task tracker
				TaskTrackerInfo ti = getTaskTrackerInfoFromNodeNum(nodeNum);
				double taskLoad = jobTable.get(jobId).mapList.get(partition-1).getLoad();
				double oldLoad = ti.getLoad();
				ti.removeLoad(taskLoad);
				double newLoad = getTaskTrackerInfoFromNodeNum(nodeNum).getLoad();
				System.out.println("Node " + nodeNum + " => Load: " + oldLoad + " to " + newLoad);
			}
			// if the map tasks were successful, check if all map tasks are done
			else {
				System.out.println("Map task " + partition + " of jobId " + jobId + " DONE.");
				
				//update jobList
				jobTable.get(jobId).mapList.get(partition-1).setStatus(2);
				int nodeNum = jobTable.get(jobId).mapList.get(partition-1).getNodeNum();
				
				//update TaskTracker load
				TaskTrackerInfo ti = getTaskTrackerInfoFromNodeNum(nodeNum);
				double taskLoad = jobTable.get(jobId).mapList.get(partition-1).getLoad();
				double oldLoad = ti.getLoad();
				ti.removeLoad(taskLoad);
				double newLoad = getTaskTrackerInfoFromNodeNum(nodeNum).getLoad();
				System.out.println("Node " + nodeNum + " => Load Dec: " + oldLoad + " to " + newLoad);
				
				//check if all Mappers are done
				int numMappers = jobTable.get(jobId).mapList.size();
				int mapFinish = 0;
				for(int i=0;i<numMappers;i++) {
					if(jobTable.get(jobId).mapList.get(i).getStatus() != 2) {
						mapFinish++;
						break;
					}
				}
				
				//start reduce phase if all mappers are complete
				if(mapFinish == 0) {
					//start reduce phase for jobId
					int numReducers = jobTable.get(jobId).getNumReducers();
					System.out.println("Map Phase has finished for JobID " + jobId);
					
					//get node assignments for reduce tasks
					System.out.println(jobTable);
					System.out.println(jobTable.get(jobId));
					String inputDir = jobTable.get(jobId).getInputDir();
					System.out.println("inputDir: " + inputDir);
					int[] reducerToNode = reducerScheduler(inputDir, numReducers, jobId);
					
					int inputDirLength = fileSystem.getFileLength(inputDir);
					
					//send mapper inputs to each reducer
					for(int i=0; i<numReducers; i++) {
						jobTable.get(jobId).reduceList.add(i, new TaskDetails(i+1, reducerToNode[i], 
								(double)inputDirLength/numReducers));
						
						//send each mapper's output to reducer
						for(int j=0; j< numMappers; j++) {
							int mapNode = jobTable.get(jobId).mapList.get(j).getNodeNum();
							int reduceNode = reducerToNode[i];
							fileSystem.sendMapOutputToNode(jobId, mapNode, j+1, reduceNode, i+1, inputDir);
						}
					}
					
					
					File[] files = new File[numMappers];
					
					//merge all reducer inputs in reduceNode
					for(int i=0; i<numReducers; i++) {
						int reduceNode = reducerToNode[i];
						String mergedFileName = "./root/" + Integer.toString(reduceNode) + "/" +
								inputDir + "_" + Integer.toString(jobId) + "red" + 
								Integer.toString(i+1);
						File mergedFile = new File(mergedFileName);
						
						for(int j=0; j<numMappers; j++) {
							int mapNode = jobTable.get(jobId).mapList.get(j).getNodeNum();
							
							String reducePartName = "./root/" + Integer.toString(mapNode) + "/" +
								inputDir + "_" + Integer.toString(jobId) + "_" + Integer.toString(j+1) +
								"out" + Integer.toString(i+1);
							File reducePart = new File(reducePartName);
							files[j] = reducePart;
						}
						
						fileSystem.mergeFiles(files, mergedFile);
						
						//sort merged file
						fileSystem.sortFile(mergedFile);
					}
					
					//start Reduce tasks
					for(int i=0; i<numReducers; i++) {
						//send Reduce class to the reduceNode
						String jarFolder = "./root" + File.separator + "mapred" + jobId;
						File reduceFile = new File(jarFolder + File.separator + "Reduce.class");
						String dirName = "job" + Integer.toString(jobId);
					
						fileSystem.sendToNodeFolder(reducerToNode[i], reduceFile, dirName);
						
						startReduceTask(jobId, numMappers, i+1, reducerToNode[i], inputDir);
					}
					
				}
			}
		} 
		/*
		 * This message is sent by each reducer when a reduce task is completed.
		 * 
		 * The JobTracker takes in the job ID of the job, the partition on which
		 * the reduce task has been completed, and the exit value of the reduce task.
		 * 
		 * It then checks the exit value to see if there were any errors in the 
		 * task. In case of errors, it informs the user of the exit value. Otherwise,
		 * it increments the count for the number of completed reduce tasks.
		 * 
		 * Once the number of completed reduce tasks reaches the total number of reducers
		 * for the particular job, the job is done!
		 */
		else if (command.equals("ReduceResult")) {
			Integer jobId = null;
			Integer partition = null;
			Integer exitValue = null;
			try {
				jobId = (Integer)ois.readObject();
				partition = (Integer)ois.readObject();
				exitValue = (Integer)ois.readObject();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			if (exitValue != 0) {
				System.out.println("Reduce task " + partition + " of jobId " + jobId + " failed.");
				
				//update jobList
				jobTable.get(jobId).reduceList.get(partition-1).setStatus(3);
				int nodeNum = jobTable.get(jobId).reduceList.get(partition-1).getNodeNum();
				
				//update TaskTracker load
				TaskTrackerInfo ti = getTaskTrackerInfoFromNodeNum(nodeNum);
				double taskLoad = jobTable.get(jobId).reduceList.get(partition-1).getLoad();
				double oldLoad = ti.getLoad();
				ti.removeLoad(taskLoad);
				double newLoad = getTaskTrackerInfoFromNodeNum(nodeNum).getLoad();
				System.out.println("Node " + nodeNum + " => Load: " + oldLoad + " to " + newLoad);
			} else {
				System.out.println("Reduce task " + partition + " of jobId " + jobId + " DONE.");
				
				//update jobList
				jobTable.get(jobId).reduceList.get(partition-1).setStatus(2);
				int nodeNum = jobTable.get(jobId).reduceList.get(partition-1).getNodeNum();
				
				//add outputFile partitions to fileSystem
				String outputDir = jobTable.get(jobId).getOutputDir();
				fileSystem.addOutputFilePart(outputDir, partition, nodeNum);
				
				//update TaskTracker load
				TaskTrackerInfo ti = getTaskTrackerInfoFromNodeNum(nodeNum);
				double taskLoad = jobTable.get(jobId).reduceList.get(partition-1).getLoad();
				double oldLoad = ti.getLoad();
				ti.removeLoad(taskLoad);
				double newLoad = getTaskTrackerInfoFromNodeNum(nodeNum).getLoad();
				System.out.println("Node " + nodeNum + " => Load Dec: " + oldLoad + " to " + newLoad);
				
				//check if all Reducers are done
				int numReducers = jobTable.get(jobId).getNumReducers();
				int reduceFinish = 0;
				for(int i=0;i<numReducers;i++) {
					if(jobTable.get(jobId).reduceList.get(i).getStatus() != 2) {
						reduceFinish++;
						break;
					}
				}
				
				//start reduce phase if all mappers are complete
				if(reduceFinish == 0) {					
					System.out.println("Reduce Phase has finished for JobID " + jobId);
				}
			}
		}
	}
	
	/*
	 * METHOD: getTaskTrackerInfoFromNodeNum
	 * INPUT: unique node number
	 * OUTPUT: information about the TaskTracker pertaining to that node number
	 * 			or null if not found.
	 * 
	 * The table of task trackers is indexed by the socket with which the 
	 * task tracker communicates. So this method iterates over all the task trackers
	 * in the table, and returns all the information about the one whose node
	 * number is supplied as input.
	 */
	static TaskTrackerInfo getTaskTrackerInfoFromNodeNum(int nodeNum) {
		TaskTrackerInfo ttiFound = null;
		
		Iterator<TaskTrackerInfo> ttiEnum = taskTrackerTable.values().iterator();
		while (ttiEnum.hasNext()) {
			TaskTrackerInfo tti = ttiEnum.next();
			if (tti.getNodeNum() == nodeNum) {
				ttiFound = tti;
				break;
			}
		}
		
		return ttiFound;
	}
	
	/*
	 * METHOD: startMapTask
	 * INPUT: the job ID, the input partition number, the node number 
	 * 			and the input directory
	 * OUTPUT: void
	 * 
	 * This method uses the node number to get the IP address and listening port
	 * of the task tracker. Then the JobTracker send that task tracker a command
	 * to run a map task on the partition of the input given by "partition" and 
	 * "inputDir" respectively.
	 * 
	 * It then waits for an acknowledgement from the task tracker to ensure that
	 * the task has been started.
	 */
	static void startMapTask(int jobId, int partition, int nodeNum, String inputDir) {
		
		// searches for the task tracker associated with unique
		// node number "nodeNum"
		TaskTrackerInfo ttiCur = getTaskTrackerInfoFromNodeNum(nodeNum);
		if (ttiCur == null) {
			System.out.println("Could not find task tracker!");
			return;
		}
		
		// get the IP address and port number of the task tracker
		String taskIP = ttiCur.getIPAddress();
		int taskPort = ttiCur.getServPort();
		
		// create a connection the the task tracker
		Socket connection = null;
		ObjectOutputStream oos = null;
		ObjectInputStream ois = null;
		try {
			connection = new Socket(taskIP, taskPort);
			
			oos = new ObjectOutputStream(connection.getOutputStream());
			oos.flush();
			ois = new ObjectInputStream(connection.getInputStream());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//get numReducers
		int numReducers = jobTable.get(jobId).getNumReducers();
		
		// send the task tracker a command to run a map task
		String command = null;
		try {
			oos.writeObject("RunMap");
			
			// supply it with the partition on which it has to run the map task
			oos.writeObject(partition);
			oos.writeObject(jobId);
			oos.writeObject(numReducers);
			oos.writeObject(inputDir);
			
			// wait for a response indicating that it has started the task
			command = (String)ois.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// if it has started the task
		if (command.equals("MapDone")) {
			System.out.println("Map has been started");
			jobTable.get(jobId).mapList.get(partition-1).setStatus(1);
		}
		// if there was an error starting the task
		else {
			System.out.println("ERROR in running Map phase");
			jobTable.get(jobId).mapList.get(partition-1).setStatus(3);
		}
	}
	
	/*
	 * METHOD: startReduceTask
	 * INPUT: unique job ID, the node number associated with the task tracker 
	 * 			on which we want to run the reduce job, input directory
	 * OUTPUT: void
	 * 
	 * Similar to the method to start a map task, this method uses the unique 
	 * node number to find the details of the task tracker pertaining to that
	 * node number, such as the listening IP address and port number.
	 * 
	 * The JobTracker then creates a connection to the task tracker, sends it
	 * any other information it needs to start a reduce task, and waits for 
	 * acknowledgement.
	 */
	static void startReduceTask(int jobId, int numMappers, int reduceNum, int reduceNode, String inputDir) {
		
		// get task tracker details from the node number.
		TaskTrackerInfo ttiCur = getTaskTrackerInfoFromNodeNum(reduceNode);
		if (ttiCur == null) {
			System.out.println("Could not find task tracker!");
			return;
		}
		
		// get IP address and port number
		String taskIP = ttiCur.getIPAddress();
		int taskPort = ttiCur.getServPort(); 
		
		// create a connection to the task tracker
		Socket connection = null;
		ObjectOutputStream oos = null;
		ObjectInputStream ois = null;
		try {
			connection = new Socket(taskIP, taskPort);
			
			oos = new ObjectOutputStream(connection.getOutputStream());
			oos.flush();
			ois = new ObjectInputStream(connection.getInputStream());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// get outputDir
		String outputDir = jobTable.get(jobId).getOutputDir();
		
		// send the command to run the reduce task,
		// along with any other information needed.
		String command = null;
		try {
			oos.writeObject("RunReduce");
			
			oos.writeObject(jobId);
			oos.writeObject(numMappers);
			oos.writeObject(reduceNum);
			oos.writeObject(inputDir);
			oos.writeObject(outputDir);
			
			// wait for acknowledgement that the reduce task has been started
			command = (String)ois.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// if the reduce task has been started
		if (command.equals("ReduceDone")) {
			System.out.println("Reduce has been finished");
			jobTable.get(jobId).reduceList.get(reduceNum-1).setStatus(1);
		}
		// if there was a problem in starting the reduce task
		else {
			System.out.println("ERROR in running Reduce phase");
			jobTable.get(jobId).reduceList.get(reduceNum-1).setStatus(3);
		}
	}
	
	/*
	 * METHOD: mapperScheduler
	 * INPUT: the input directory, the number of mappers and the unique job ID
	 * OUTPUT: an array that is a mapping from an integer to a node number, with
	 * 			one mapping for each Map task
	 * 
	 * This method creates a min-priority-queue with comparisons done based on
	 * the amount of load on each TaskTracker. The mapping is then created by
	 * repeatedly picking the min-element from the priority queue IF it has the partition
	 * of the input data on it. This way, we choose those TaskTrackers to be the mappers 
	 * that have the least load on them, and to which the data has already been sent. 
	 */
	static int[] mapperScheduler(String inputDir, int numMappers, int jobID) {
		
		int[] mapperToNode = new int[numMappers];
		
		for (int i=0; i<numMappers; i++) {
			// Priority queue for scheduler
			Comparator<TaskTrackerInfo> ttiComparator = new TaskTrackerInfoComparator();
			PriorityQueue<TaskTrackerInfo> ttiQueue = new PriorityQueue<TaskTrackerInfo>(10, ttiComparator);
			
			// Put all the task trackers into the priority queue which have partNum of inputDir
			Iterator<TaskTrackerInfo> ttiEnum = taskTrackerTable.values().iterator();
			while (ttiEnum.hasNext()) {
				TaskTrackerInfo tti = ttiEnum.next();
				//add only if it has the partition of the inputDir
				if(fileSystem.checkFilePartInNode(inputDir, i+1, tti.getNodeNum()) == 1) {
					ttiQueue.add(tti);
				}
			}
			
			//System.out.println(ttiQueue.size());
			
			//get inputDir's size
			int inputDirLength = fileSystem.getFileLength(inputDir);
			
			//add the TaskTracker with the least load
			TaskTrackerInfo ttiMin = ttiQueue.poll();
			ttiMin.addLoad((double)inputDirLength/numMappers);
			//ttiMin.addLoad(jobTable.get(jobId).mapList.get(i).getLoad());
			mapperToNode[i] = ttiMin.getNodeNum();
			ttiQueue.add(ttiMin);
		}
		
		return mapperToNode;
	}
	
	/*
	 * METHOD: reducerScheduler
	 * INPUT: the input directory, the number of reducers and the unique job ID
	 * OUTPUT: an array that is a mapping from an integer to a node number, with
	 * 			one mapping for each Reduce task
	 * 
	 * This method creates a min-priority-queue with comparisons done based on
	 * the amount of load on each TaskTracker. The mapping is then created by
	 * repeatedly picking the min-element from the priority queue. This way, we
	 * choose those TaskTrackers to be the reducers that have the least load on them.
	 * 
	 * It is very similar to the scheduling done on the Map phase, except now we 
	 * don't care about what TaskTrackers have what partitions of data on them.
	 */
	static int[] reducerScheduler(String inputDir, int numReducers, int jobID) {
		
		int[] reducerToNode = new int[numReducers];
		
		for (int i=0; i<numReducers; i++) {
			// Priority queue for scheduler
			Comparator<TaskTrackerInfo> ttiComparator = new TaskTrackerInfoComparator();
			PriorityQueue<TaskTrackerInfo> ttiQueue = new PriorityQueue<TaskTrackerInfo>(10, ttiComparator);
			
			// Put all the task trackers into the priority queue
			Iterator<TaskTrackerInfo> ttiEnum = taskTrackerTable.values().iterator();
			while (ttiEnum.hasNext()) {
				TaskTrackerInfo tti = ttiEnum.next();
				//add only if it has the partition of the inputDir
				ttiQueue.add(tti);
			}
			
			//get inputDir's size
			int inputDirLength = fileSystem.getFileLength(inputDir);
			
			//add the TaskTracker with the least load
			TaskTrackerInfo ttiMin = ttiQueue.poll();
			ttiMin.addLoad((double)inputDirLength/numReducers);
			//ttiMin.addLoad(jobTable.get(jobId).mapList.get(i).getLoad());
			reducerToNode[i] = ttiMin.getNodeNum();
			ttiQueue.add(ttiMin);
		}
		
		return reducerToNode;
	}
	
	/*
	 * METHOD: extractJAR
	 * INPUT: job ID, to construct the name of the folder to be extracted in
	 * OUTPUT: void
	 * 
	 * This method reads and extracts the JAR file into a specially named folder.
	 * The JAR file consists of the Map and Reduce classes.
	 */
	void extractJAR(int jobId) {
		int bufferSize = 0;
		String destDir = "./root";
		
		// the folder where the JAR file will be extracted
		String jarFolder = destDir + File.separator + "mapred" + jobId;
		
		File destDirF = new File(jarFolder);
		if (destDirF.mkdir()) {
			System.out.println("Created mapred jar directory for job " + jobId);
		} else {
			System.out.println("Could not create mapred jar directory for job " + jobId);
			System.exit(1);
		}
		
		System.out.println(jobId + ":" + jarFolder + File.separator + "mapred" + jobId + ".jar");
		
		InputStream is = null;
		try {
			is = newConnection.getInputStream();
			bufferSize = newConnection.getReceiveBufferSize();
			System.out.println("Buffer size: " + bufferSize);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		FileOutputStream fos = null;
		BufferedOutputStream bos = null;
		try {
			fos = new FileOutputStream(jarFolder + File.separator + 
					"mapred" + jobId + ".jar");
			bos = new BufferedOutputStream(fos);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		byte[] bytes = new byte[bufferSize];
		
		int count;
		
		// reads the JAR file and writes it into the "jarFolder"
		try {
			while ((count = is.read(bytes)) > 0) {
				bos.write(bytes, 0, count);
			}
			
			bos.flush();
			bos.close();
			is.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// use JarFile class to handle extraction of the JAR file
		JarFile jar = null;
		try {
			jar = new JarFile(jarFolder + File.separator + "mapred" + jobId + ".jar");
		} catch (Exception e) {
			e.printStackTrace();
		}
		Enumeration enumEntries = jar.entries();
		
		int numFilesExtracted = 0;
		
		// iterate through the files in the JAR file, and extract them
		try {
			while (enumEntries.hasMoreElements()) {
				JarEntry file = (JarEntry) enumEntries.nextElement();
				File fExtractor = new File(jarFolder + File.separator + file.getName());
				System.out.println(fExtractor.getName());
				
				// ignore certain files
				if (fExtractor.getName().equals("META_INF") || 
						fExtractor.getName().equals("MANIFEST.MF")) {
					continue;
				}
				
				if (file.isDirectory()) {
					fExtractor.mkdir();
					continue;
				}

				InputStream isExtractor = jar.getInputStream(file);
				FileOutputStream fosExtractor = new FileOutputStream(fExtractor);
				
				// write the file to the "jarFolder"
				System.out.println(isExtractor.available());
				while (isExtractor.available() > 0) {
					fosExtractor.write(isExtractor.read());
				}
				
				fosExtractor.close();
				isExtractor.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//Failure Handling for Tasks when TaskTracker fails
	static void nodeFailureTolerance(int nodeNum) {
		//Set of all Jobs in jobTable
		Set<Integer> jobIds = jobTable.keySet();
		
		for(Integer jobId: jobIds){
            Job jobEntry = jobTable.get(jobId);
            
            int numMappers = jobEntry.getNumMappers();
            int numReducers = jobEntry.getNumReducers();
            
            String inputDir = jobEntry.getInputDir();
            String outputDir = jobEntry.getOutputDir();
            
            for(int i=0; i<numMappers; i++) {
            	int taskStatus = jobEntry.mapList.get(i).getStatus();
            	int taskNode = jobEntry.mapList.get(i).getNodeNum();
            	int taskAttempt = jobEntry.mapList.get(i).getAttempt();
            	
            	if(taskNode == nodeNum && taskAttempt<2) {
            		if(taskStatus == 0 || taskStatus == 1) {
            			System.out.println("Mapper " + (i+1) + " of jobID " + jobId
            					+ " failed. Task restarting on another node!");
            			
            			//get node from mapperScheduler
                		int[] mapperToNode = mapperScheduler(inputDir, 1, jobId);
                		
                		//change nodeNum and status in TaskDetails
                		jobTable.get(jobId).mapList.get(i).changeNodeNum(mapperToNode[0]);
                		jobTable.get(jobId).mapList.get(i).setStatus(4);
                		
                		//start Map Task
                		startMapTask(jobId, i+1, mapperToNode[0], inputDir);
            		}
            	}
            }
            
            for(int i=0; i<numReducers; i++) {
            	int taskStatus = jobEntry.reduceList.get(i).getStatus();
            	int taskNode = jobEntry.reduceList.get(i).getNodeNum();
            	int taskAttempt = jobEntry.reduceList.get(i).getAttempt();
            	
            	if(taskNode == nodeNum && taskAttempt<2) {
            		if(taskStatus == 0 || taskStatus == 1) {
            			System.out.println("Reducer " + (i+1) + " of jobID " + jobId
            					+ " failed. Task restarting on another node!");
            			
            			//get node from mapperScheduler
                		int[] reducerToNode = reducerScheduler(inputDir, 1, jobId);
                		
                		//change nodeNum and status in TaskDetails
                		jobTable.get(jobId).reduceList.get(i).changeNodeNum(reducerToNode[0]);
                		jobTable.get(jobId).reduceList.get(i).setStatus(4);
                		
                		//start Map Task
                		startReduceTask(jobId, numMappers, i+1, reducerToNode[0], inputDir);
            		}
            	}
            }
        }
	}
}

/* Poll data nodes for their status*/
class PollClient implements Runnable {
	public void run() {		
		while(true) {
			/*Enumeration for all nodes*/
			Enumeration<Socket> ttiEnum = JobTracker.taskTrackerTable.keys();
			
			/*Iterate through enumeration of nodes for polling*/
			while (ttiEnum.hasMoreElements()) {
				Socket s = ttiEnum.nextElement();
				int port = JobTracker.taskTrackerTable.get(s).getPollPort();
				String ip = s.getInetAddress().getHostAddress();
				
				Socket connection = null;
				ObjectOutputStream oos = null;
				ObjectInputStream ois = null;
				String command = null;
				try {
					connection = new Socket(ip, port);
					
					/*wait for timeout for reply*/
					connection.setSoTimeout(60*1000);
					
					oos = new ObjectOutputStream(connection.getOutputStream());
					oos.flush();
					ois = new ObjectInputStream(connection.getInputStream());
					oos.writeObject("Poll");
					command = (String)ois.readObject();
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				/* remove if node not responding*/
				if (command == null) {
					//nodeNum to be removed
					int nodeNum = JobTracker.taskTrackerTable.get(s).getNodeNum();
					//remove files of node from FileSystem
					JobTracker.fileSystem.removeNode(nodeNum);
					//remove node from TaskTrackerTable
					JobTracker.taskTrackerTable.remove(s);
					System.out.println("Node with nodeNum " + nodeNum + " has failed and has been removed");
					
					//TODO:ADD Fault Tolerance here
					JobTracker.nodeFailureTolerance(nodeNum);
				}
				else {
					System.out.println(command);
				}

				/* Close socket after receiving the reply*/
				try {
					connection.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			
			/*wait for some time before repolling all nodes*/
			try {
				Thread.sleep(10*1000);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}