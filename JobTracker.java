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
	static FileSystem fileSystem = new FileSystem();
	
	// Table of jobs indexed by jobId
	Hashtable<Integer, Job> jobTable = new Hashtable<Integer, Job>();
	
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
		
		int jobTrackerPort = Integer.parseInt(args[0]);
		
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

			/*Instantiate a new Thread for listening to connections*/		
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
		//input and output streams for newConnection
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
		
		/* Read Request*/
		try {
			command = (String)ois.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/* New TaskTracker*/
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

			//add node to FileSystem
			fileSystem.addNode(taskTrackerInfo);

			nodeNum++;
		} else if (command.equals("ListTaskTrackers")) {
			/* send a List TaskTracker(s) to MapReduce*/
			ArrayList<TaskTrackerInfo> taskTrackerInfo = 
					new ArrayList<TaskTrackerInfo>(taskTrackerTable.values());
			
			try {
				oos.writeObject(taskTrackerInfo);
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("JobTracker replied to ListTaskTracker.");
		} else if (command.equals("copy")) {
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
				/*ADD checks if dest dir exists is Dist. FILESYSTEM here*/
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
		} else if (command.equals("delete")) {
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
				/*ADD check if dest dir exists is Dist. FILESYSTEM here*/
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
		} else if (command.equals("NewJob")) {
			
			jobId++;
			String inputDir = null;
			
			try {
				inputDir = (String)ois.readObject();
			} catch (Exception e) { 
				e.printStackTrace();
			}
			
			boolean valid = false;
			try {
				if (fileSystem.filePresent(inputDir) == 0) {
					oos.writeObject("NotExist");
				}
				
				valid = true;
				oos.writeObject("AckDir");
				System.out.println("Input directory for new job: " + inputDir);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			if (valid) { // If the job is valid
				int numMappers = 3;
				int numReducers = 3;
				// Extract the .jar
				extractJAR(jobId);
				
				// creat new job and add to jobTable
				Job newJob = new Job(numMappers, numReducers, jobId, inputDir);
				jobTable.put(jobId, newJob);
				
				String destDir = "./root";
				String jarFolder = destDir + File.separator + "mapred" + jobId;
				
				// scheduler for mapper stage
				int[] mapperToNode = mapperScheduler(inputDir, numMappers, jobId);
				fileSystem.splitToPartitions(inputDir, numMappers, jobId);
				
				int inputDirLength = fileSystem.getFileLength(inputDir);
				
				for (int i=0; i<numMappers; i++) { // loop over mappers, start them
					jobTable.get(jobId).mapList.add(i, new TaskDetails(i+1, mapperToNode[i], 
							(double)inputDirLength/numMappers));
					
					fileSystem.sendPartitionToNode(inputDir, i+1, mapperToNode[i], jobId);
					
					File mapFile = new File(jarFolder + File.separator + "Map.class");
					String dirName = "job" + Integer.toString(jobId);
					
					fileSystem.sendToNodeFolder(mapperToNode[i], mapFile, dirName);
					startMapTask(jobId, i+1, mapperToNode[i], inputDir);
				}
			}
		} else if (command.equals("MapResult")) {
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
				System.out.println("Map task " + partition + " of jobId " + jobId + " failed.");
				
				//update jobList
				jobTable.get(jobId).mapList.get(partition-1).setStatus(3);
				int nodeNum = jobTable.get(jobId).mapList.get(partition-1).getNodeNum();
				
				//update TaskTracker load
				TaskTrackerInfo ti = getTaskTrackerInfoFromNodeNum(nodeNum);
				double taskLoad = jobTable.get(jobId).mapList.get(partition-1).getLoad();
				double oldLoad = ti.getLoad();
				ti.removeLoad(taskLoad);
				double newLoad = getTaskTrackerInfoFromNodeNum(nodeNum).getLoad();
				System.out.println("Node " + nodeNum + " => Load: " + oldLoad + " to " + newLoad);
			} else {
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
				System.out.println("Node " + nodeNum + " => Load: " + oldLoad + " to " + newLoad);
				
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
					int numReducers = 3;
					System.out.println("Map Phase has finished for JobID " + jobId);
					
					//get node assignments for reduce tasks
					String inputDir = jobTable.get(jobId).getInputDir();
					int[] reducerToNode = mapperScheduler(inputDir, numReducers, jobId);
					
					int inputDirLength = fileSystem.getFileLength(inputDir);
					
					for (int i=0; i<numReducers; i++) { // loop over reducers, start them
						jobTable.get(jobId).reduceList.add(i, new TaskDetails(i+1, reducerToNode[i], 
								(double)inputDirLength/numReducers));
						
						//TODO: ask each mapper to send output to reducer i
						/*
						fileSystem.sendMapOutputToNode(inputDir, i+1, reducerToNode[i], jobId);
						
						File mapFile = new File(jarFolder + File.separator + "Map.class");
						String dirName = "job" + Integer.toString(jobId);
						
						fileSystem.sendToNodeFolder(mapperToNode[i], mapFile, dirName);
						startMapTask(jobId, i+1, mapperToNode[i], inputDir);
						*/
					}
				}
			}
		}
	}
	
	TaskTrackerInfo getTaskTrackerInfoFromNodeNum(int nodeNum) {
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
	
	void startMapTask(int jobId, int partition, int nodeNum, String inputDir) {
		TaskTrackerInfo ttiCur = getTaskTrackerInfoFromNodeNum(nodeNum);
		if (ttiCur == null) {
			System.out.println("Could not find task tracker!");
			return;
		}
		
		String taskIP = ttiCur.getIPAddress();
		int taskPort = ttiCur.getServPort(); 
		
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
		
		String command = null;
		try {
			oos.writeObject("RunMap");
			oos.writeObject(partition);
			oos.writeObject(jobId);
			oos.writeObject(inputDir);
			
			command = (String)ois.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if (command.equals("MapDone")) {
			System.out.println("Map has been finished");
			jobTable.get(jobId).mapList.get(partition-1).setStatus(1);
		} else {
			System.out.println("ERROR in running Map phase");
			jobTable.get(jobId).mapList.get(partition-1).setStatus(3);
		}
	}
	
	/*
	 * TODO: change name to taskScheduler
	 */
	int[] mapperScheduler(String inputDir, int numMappers, int jobID) {
		// Priority queue for scheduler
		Comparator<TaskTrackerInfo> ttiComparator = new TaskTrackerInfoComparator();
		PriorityQueue<TaskTrackerInfo> ttiQueue = new PriorityQueue<TaskTrackerInfo>(10, ttiComparator);
		
		// Put all the task trackers into the priority queue
		Iterator<TaskTrackerInfo> ttiEnum = taskTrackerTable.values().iterator();
		while (ttiEnum.hasNext()) {
			TaskTrackerInfo tti = ttiEnum.next();
			ttiQueue.add(tti);
		}
		
		//System.out.println(ttiQueue.size());
		
		int inputDirLength = fileSystem.getFileLength(inputDir);
		
		int[] mapperToNode = new int[numMappers];
		for (int i=0; i<numMappers; i++) {
			TaskTrackerInfo ttiMin = ttiQueue.poll();
			ttiMin.addLoad((double)inputDirLength/numMappers);
			//ttiMin.addLoad(jobTable.get(jobId).mapList.get(i).getLoad());
			mapperToNode[i] = ttiMin.getNodeNum();
			ttiQueue.add(ttiMin);
		}
		
		return mapperToNode;
	}
	
	void extractJAR(int jobId) {
		int bufferSize = 0;
		String destDir = "./root";
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
		
		JarFile jar = null;
		try {
			jar = new JarFile(jarFolder + File.separator + "mapred" + jobId + ".jar");
		} catch (Exception e) {
			e.printStackTrace();
		}
		Enumeration enumEntries = jar.entries();
		
		int numFilesExtracted = 0;
		try {
			while (enumEntries.hasMoreElements()) {
				JarEntry file = (JarEntry) enumEntries.nextElement();
				File fExtractor = new File(jarFolder + File.separator + file.getName());
				System.out.println(fExtractor.getName());
				
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
	
	public static int numPart(File file) {
		return (int)Math.ceil((file.length())/(1024*1024));
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
					//System.out.println("Wut1");
					//nodeNum to be removed
					int nodeNum = JobTracker.taskTrackerTable.get(s).getNodeNum();
					//remove files of node from FileSystem
					JobTracker.fileSystem.removeNode(nodeNum);
					//remove node from TaskTrackerTable
					JobTracker.taskTrackerTable.remove(s);
					System.out.println("Node with nodeNum " + nodeNum + " has failed and has been removed");
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