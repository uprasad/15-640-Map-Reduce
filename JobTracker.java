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
			
			File f = new File(inputDir);
			/*
			 * BAD DIRECTORY CHECKING!
			 */
			boolean valid = false;
			try {
				if (!f.exists()) {
					oos.writeObject("NotExist");
				}
			
				if (!f.isDirectory()) {
					oos.writeObject("NotDir");
				}
				
				valid = true;
				oos.writeObject("AckDir");
				System.out.println("Input directory for new job: " + inputDir);
			} catch (Exception e) {
				e.printStackTrace();
			}
			/*
			 * END OF BAD CHECKING
			 */
			
			if (valid) {
				// Done reading the .jar
				
				// Extract the .jar
				extractJAR(jobId);
			}
		}
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