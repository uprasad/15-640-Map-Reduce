import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

public class FileSystem {
	/*
	 * fileSystem for Directory names
	 * 1. hashed by the directory names
	 * 2. fileEntry object for partition and replica infos
	 */
	static Hashtable<String, FileEntry> fileTable = new Hashtable<String, FileEntry>();
	
	/*
	 * list of active nodes(TaskTracker) in cluster
	 */
	static List<Integer> nodeList = new ArrayList<Integer>();
	
	/*
	 * nodeInfo for every node(TaskTracker) in the cluster
	 * 1. hashed by nodeNum of the TaskTracker
	 * 2. TaskTrackerInfo object for network details of the TaskTracker
	 *    and other info, such as load etc.
	 */
	static Hashtable<Integer,TaskTrackerInfo> nodeInfo = new Hashtable<Integer,TaskTrackerInfo>();
	
	//JobTracker network details
	static String jobTrackerIP = null;
	static int jobTrackerPort;
	
	/*
	 * constructor which initializes the fileTable
	 * 1. copies network details for JobTracker
	 * 2. creates a root directory for the FileSystem
	 */
	FileSystem(String ip, int port) {
		this.jobTrackerIP = ip;
		this.jobTrackerPort = port;
		createRoot();
	}
	
	/*
	 * 1. createRoot() method sets up a new root directory
	 * for the FileSystem.
	 * 2. The root directory contains one directory for each
	 * node in the cluster, to which the files are distributed into.
	 */
	static void createRoot() {
		/*Create root folder for DFS*/
        File dir = new File("root");
        
        /*DFS root not existing*/
        if(!dir.exists()) {
        	if(dir.mkdir()) {
        		System.out.println("Root directory for DFS created");
            }
            else {
            	System.out.println("Root directory for DFS could not be created. Exiting!");
                System.exit(1);
            }
        } else {
        	/*DFS root exists, delete old root*/
            delete(dir);
            System.out.println("Existing Root for DFS deleted");

            /*Replace the existing root by new root directory*/
            if(dir.mkdir()) {
            	System.out.println("New Root directory for DFS created");
            }
            else {
            	System.out.println("New Root directory for DFS could not be created. Exiting!");
                System.exit(1);
            }
        }
	}
	
	/*
	 * delete() method deletes a file object recursively, i.e.
	 * if the input file object is a directory, it deletes all the
	 * sub-directories and files inside.
	 * 
	 * Since Java does not allow to delete non -empty directories directly,
	 * delete() first deletes all files in a directory, and then finally
	 * deletes the now empty directory.
	 */
	static void delete(File file) {
		/*check if file is a directory*/
		if(file.isDirectory()) {
			/*delete empty directory*/
			if(file.list().length == 0) {
				file.delete();
				System.out.println(file.getAbsolutePath() + " deleted");
			} else {
				/* delete all files inside directory recursively*/
				String files[] = file.list();
				for(String s: files) {
					File fileDelete = new File(file, s);
					delete(fileDelete);
				}

				/*delete directory after it is empty*/
				if(file.list().length == 0) {
					file.delete();
					System.out.println(file.getAbsolutePath() + " deleted");
				}
			}
		} else {
			/*delete directly if not a directory*/
			file.delete();
			System.out.println(file.getAbsolutePath() + " deleted");
		}
	}
	
	/*
	 * addNode() adds a TaskTracker to the existing FileSystem:
	 * 1. Add the taskTrackerInfo to the nodeList and the nodeInfo tables
	 * 2. Create a directory inside ./root/ with path ./root/nodeNum
	 * 	  where nodeNum is the nodeNum for TaskTracker
	 */
	static void addNode(TaskTrackerInfo taskTrackerInfo) {
		//add to nodeList and nodeInfo
		int nodeNum = taskTrackerInfo.getNodeNum();
		
		nodeList.add(nodeNum);
		try {
			nodeInfo.put(nodeNum, taskTrackerInfo);
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		//path for TaskTracker directory in the FileSystem
		String dirName = "./root/" + Integer.toString(nodeNum);
		File dir = new File(dirName);
		
		//if directory already exists for nodeNum, delete it
		if(dir.exists()) {
			delete(dir);
			System.out.println("Existing DFS Directory for node " + nodeNum + " deleted.");
		}
    	
		//create new directory for the nodeNum in the ./root/ path
		if(dir.mkdir()) {
			System.out.println("New DFS Directory for node " + nodeNum + " created");
	    }
        else {
        	System.out.println("New DFS Directory for node " + nodeNum + " could not be created. Exiting");
        	System.exit(1);
        }
	}
	
	/*
	 * removeNode() removes a TaskTracker from the existing FileSystem:
	 * 1. remove TaskTracker with nodeNum from nodeList and nodeInfo
	 * 2. remove the associated ./root/nodeNum directory and its files
	 *    from the root of the FileSystem
	 */
	static void removeNode(int nodeNum) {
		//remove TaskTracker from nodeList
		Integer nodeToRemove = new Integer(nodeNum);
		nodeList.remove(nodeToRemove);
		
		//remove TaskTracker from nodeInfo
		try {
			nodeInfo.remove(nodeNum);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//existing path of the TaskTracker directory  in the FileSystem
		String dirName = "./root/" + Integer.toString(nodeNum);
		File dir = new File(dirName);
		
		//delete the TaskTracker directory from the FileSystem
		if(dir.exists()) {
			delete(dir);
			System.out.println("DFS directory for failed Node " + nodeNum + " removed");
		}
		
		/*
		 * remove all the replica information from the nodeNum, i.e.
		 * dereference all replicas from the FileSystem for files/directories
		 * stored in the deleted node.
		 */
		deleteNodeFiles(nodeNum);
	}
	
	/*
	 * deleteNodeFiles() removes all the FileEntries in the FileSystem
	 * which have replicas in the node given by the nodeNum input.
	 * 
	 * Hence, it is used to remove references for replicas which were in
	 * the TaskTracker node given by the nodeNum.
	 */
	static void deleteNodeFiles(int nodeNum) {
		//get all directories/files in the FileSystem
		Set<String> keys = fileTable.keySet();
		
		//iterate through all entries in the FileTable
        for(String key: keys){
            FileEntry fileEntry = fileTable.get(key);
            
            //check all partitions of the file
            for(int p=0; p<fileEntry.getNumParts(); p++) {
	            //check all replicas of the partition
            	for(int i=0; i<fileEntry.getRFactor(); i++) {
	        		//dereference replica entry from the fileEntry object
            		if(fileEntry.getEntry(p, i) == nodeNum) {
	        			fileEntry.editList(p, i, -1); //replica missing due to failure
	        		}
	            }
            }
        }
	}
	
	/*
	 * copyFiles() method copies files from scrDir path to the
	 * destDir path on the DFS using the newtork
	 */
	static void copyFiles(File srcDir, String destDir) {
		File[] listOfFiles = srcDir.listFiles();
		
		for (int i=0; i<listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				System.out.println("Input file: " + listOfFiles[i].getName());
				sendToJobTrackerFolder(listOfFiles[i], destDir);
			}
		}
	}
	
	/*
	 * addFile() adds the directory with fileName to
	 * the DFS and returns 0 if succesful:
	 * 1. checks if the source with fileName exists, and returns 1 if
	 * 	  it's not valid.
	 * 2. if the source is not an input directory, but a file, it
	 * 	  returns 2.
	 * 3. checks if there is a previous entry in the fileSystem with
	 * 	  the same fileName, and returns 3 in that case.
	 */
	static int addFile(String fileName) {
		File srcDir = new File(fileName);

		//check if the source exists
		if(!srcDir.exists()) {
			return 1;
		} else if(!srcDir.isDirectory()) {
			//check if source is a directory
			return 2;
		}
		
		if(fileTable.containsKey(srcDir.getName())) {
			//check if it's already present in the FileSystem
			return 3;
		} else {
			//temp copy to root
			String dest = "./root/" + srcDir.getName();
			File destDir = new File(dest);
			
			//copy files from srcDir to dest
			copyFiles(srcDir, dest);
			
			//add from temp to fileTable
			addEntry(destDir);
			
			//remove temp copy
			//delete(destDir);
		}
		
		return 0;
	}
	
	/*
	 * deleteFile() removes the entry with fileName from the cluster
	 * and returns 0 on successful deletion:
	 * 1. if the fileName is missing from the DFS, it returns 1
	 * 2. removes all replicas from the DFS, and updates references
	 *    in the FileSystem 
	 */
	static int deleteFile(String fileName) {
		//check for presence in FileSystem
		if(!fileTable.containsKey(fileName)) {
			return 1;
		}
		
		//get numParts (no. of partitions) and rFactor (replicas)
		int numParts = fileTable.get(fileName).getNumParts();
		int rFactor = fileTable.get(fileName).getRFactor();
		
		//iterate through all partitions
		for(int p=0; p<numParts; p++) {
			//iterate through all replicas
			for(int i=0; i<rFactor; i++) {
				//get TaskTracer nodeNum for replica
				int nodeNum = fileTable.get(fileName).getEntry(p, i);
				
				//delete from DFS
				if(nodeNum>0) {
					String deleteFileName = "./root/" + Integer.toString(nodeNum) + "/" + fileName
							+ Integer.toString(p+1);
					File delFile = new File(deleteFileName);
					
					delete(delFile);
				}
			}
		}
		
		//delete fileEntry from fileTable
		fileTable.remove(fileName);
		
		return 0;
	}
	
	/*
	 * copyDir() copies a src directory into the dest directory
	 * NOTE: Not used after initial version, as files then were
	 * 		 copied over the network.
	 */
	static void copyDir(File src, File dest) {
		//if dest directory is already present
		if(dest.exists()) {
			System.out.println(dest.getName() + " already exists in the DFS");
		} else {
			//if source src is a directory
			if(src.isDirectory()) {
				dest.mkdir();
				System.out.println(dest.getName() + " created");

				String files[] = src.list();
				
				//copy all files from src to dest in the directory
				for(String file: files) {
					File srcFile = new File(src,file);
					File destFile = new File(dest,file);
					
					copyDir(srcFile,destFile);
				}
			} else {
				//copy file, i.e. not a directory
				
				//input and output streams for writing the file
				InputStream in = null;
				OutputStream out = null;
				try {
					in = new FileInputStream(src);
					out = new FileOutputStream(dest);
				} catch (Exception e) {
					e.printStackTrace();
				}

				byte[] buffer = new byte[1024];

				int length;
			
				//write src to dest for length of file using a buffer
				try {
					while ((length = in.read(buffer)) > 0){
						out.write(buffer, 0, length);
					}

					in.close();
					out.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
	
				System.out.println("File copied from " + src + " to " + dest);
			}
		}
	}
	
	/*
	 * addEntry() is a helper method for addFile()
	 * 1. checks if directory already exists in the DFS
	 * 2. merge all files in the directory into a single file
	 * 3. split the merged files into partitions (of 3MB chuncks)
	 * 4. replicate these partitions to rFactor (3 replication factor) no. of nodes
	 * 5. add all partition and replication info. to the fileTable 
	 */
	static void addEntry(File destDir) {
		//check if destDir already exists
		if(!destDir.exists()) {
			System.out.println(destDir.getName() + " couldn't be found in /root");
			return;
		}
		
		//get all files in the destDir
		String fileNames[] = destDir.list();
		int numFiles = 0;
		
		//check for number of files in the directory
		for(String fileName: fileNames) {
			fileName = destDir.getAbsolutePath() + "/" + fileName;
			File file = new File(fileName);
			if(!file.isDirectory()) {
				numFiles++;
			}
		}
		
		File[] files = new File[numFiles];
		int k=0;
		
		//populate files in files[] array
		for(String fileName: fileNames) {
			fileName = destDir.getAbsolutePath() + "/" + fileName;
			File file = new File(fileName);
			if(!file.isDirectory()) {
				files[k++] = file;
				System.out.println(file.getAbsolutePath());
			}
		}
		
		//temp mergedFile name
		String mergedName = "./root/" + destDir.getName() + "/kgabbita_udbhavp_file";
		File mergedFile = new File(mergedName);
		
		//merge files[] into mergedFile
		mergeFiles(files, mergedFile);
		
		//delete other files
		for(String fileName: fileNames) {
			fileName = destDir.getAbsolutePath() + "/" + fileName;
			File file = new File(fileName);
			if(!file.getName().equals("kgabbita_udbhavp_file")) {
				delete(file);
			}
		}
		
		int rFactor = 3; // number of replicas created
		//int rFactor = 1; // for testing purposes
		
		/*
		 * Change name of mergedFile to original directory name
		 */
		String newFileName = "./root/" + destDir.getName() + "/" + destDir.getName();
		File newFile = new File(newFileName);
		boolean mergeStatus = mergedFile.renameTo(newFile);
		if(mergeStatus) {
			System.out.println("Files in " + destDir.getName() + " merged successfully");
		} else {
			System.out.println("Files in " + destDir.getName() + " could not be merged successfully");
		}
		
		//split newFile into numParts with prefix destDir's name
		int fileLength = (int)newFile.length();
		int numParts = (int)Math.ceil((double)fileLength/(3*1024*1024)); //split to 3MB chunks
		splitFiles(newFile, destDir.getName(), numParts);
		
		//check if nodes < replication factor
		if(nodeList.size() < rFactor) {
			System.out.println("Number of Nodes is less than the replication factor of " + rFactor);
			System.out.println(destDir.getName() + " could not be added");
		} else {
			//add to fileTable
			FileEntry fileEntry = new FileEntry(destDir.getName(), numParts, rFactor, fileLength);
			fileTable.put(destDir.getName(), fileEntry);
			
			//send each partition to replica no. of nodes
			for(int p=0; p<numParts; p++) {
				//randomNumbers list to decide nodes for replicas
				ArrayList<Integer> randomNumbers = new ArrayList<Integer>();
				for(int i = 0; i<(nodeList.size()); i++) {
					randomNumbers.add(i+1);
				}
				Collections.shuffle(randomNumbers);
				
				for(int i=0; i<rFactor; i++) {
					//partionFileName and File object
					String fileName = destDir.getAbsolutePath() + "/" + destDir.getName() + Integer.toString(p+1);
					File file = new File(fileName);
					
					//make replicaCopy with name appended
					//String replicaCopyName = fileName + Integer.toString(i+1);
					//File replicaCopy = new File(replicaCopyName);
					//copyDir(file, replicaCopy);				
					
					//send replicas to nodes
					int node = randomNumbers.get(i);
					System.out.println("Partition " + (p+1) + ", Copy " + (i+1) + " => " + "Node " + node);
					sendToNode(node, file);
					fileTable.get(destDir.getName()).editList(p, i, node);
				}
			}
				
			//remove replicaCopies
			String replicaFileNames[] = destDir.list();
			for(String replicaFileName: replicaFileNames) {
				replicaFileName = destDir.getAbsolutePath() + "/" + replicaFileName;
				File file = new File(replicaFileName);
				if(!file.getName().equals(destDir.getName())) {
					delete(file);
				}
			}
		}
	}
	
	/*
	 * sorts a file by keys (first column) lexicographically
	 * NOTE: uses the UNIX sort
	 */
	static void sortFile(File file) {
		String absolutePath = file.getAbsolutePath();
		ProcessBuilder pb = new ProcessBuilder("sort", absolutePath, "-o", absolutePath);
		try {
			Process p = pb.start();
			p.waitFor();
		} catch (Exception e) {
			System.out.println("Error is sorting the Map output.");
		}
	}
	
	/*
	 * mergeFiles() merges all files in files[] to a single
	 * mergedFile
	 */
	static void mergeFiles(File[] files, File mergedFile) {
		//file i/p and o/p streams
		FileWriter fstream = null;
		BufferedWriter out = null;
		try {
			fstream = new FileWriter(mergedFile, true);
			out = new BufferedWriter(fstream);
		} catch (Exception e) {
			e.printStackTrace();
		}
 
		//iterate through all files in files[]
		for (File f : files) {
			System.out.println("merging: " + f.getName());
			FileInputStream fis;
			//write all files[] into mergedFile
			try {
				fis = new FileInputStream(f);
				BufferedReader in = new BufferedReader(new InputStreamReader(fis));
 
				String aLine;
				while ((aLine = in.readLine()) != null) {
					out.write(aLine);
					out.newLine();
				}
 
				in.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
 
		//close the output stream
		try {
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * filePresent() checks if fileName is present in the
	 * FileSystem, i.e. fileTable:
	 * 1. returns 1 if present
	 * 2. else, returns 0 if absent
	 */
	static int filePresent(String fileName) {
		int result = 0;
		
		File file = new File(fileName);
		String formattedFileName = file.getName();
		
		if(fileTable.containsKey(formattedFileName)) {
			result = 1;
		}
		
		return result;
	}
	
	/*
	 * splitToPartitions() splits a file with fileName into numParts
	 * with the suffix of jobID.
	 * 
	 * NOTE: Uses the splitFiles() method to evebtually split the file
	 */
	static void splitToPartitions(String fileName, int numParts, int jobID) {
		//check if file is present in the DFS
		if(filePresent(fileName) == 0) {
			System.out.println(fileName + " not in DFS!");
			return;
		}
		
		//dummyFile object for getting formatted fileName
		File dummyFile = new File(fileName);
		String formattedFileName = dummyFile.getName();
		
		formattedFileName = "./root/" + formattedFileName + "/" + formattedFileName;
		File file = new File(formattedFileName);
		
		//partition prefix name for splitFiles()
		String partitionName = file.getName() + "_" + Integer.toString(jobID) + "_";
		
		splitFiles(file, partitionName, numParts);
		
		System.out.println("Files partitioned for " + formattedFileName);
	}
	
	/*
	 * splitFiels() splits a file into numParts partitions with
	 * a custom prefix for split fileName taken as input.
	 */
	static void splitFiles(File file, String prefix, int numParts) {
		//lines per partition of file
		int linePerPartition = (int)Math.ceil((float)numLines(file)/numParts);
		
		//current partition number
		int filePart = 1;
		
		//streams for input file
		FileInputStream fis = null;
		BufferedReader in = null;
		try {
			fis = new FileInputStream(file);
			in = new BufferedReader(new InputStreamReader(fis));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//partitioning
		while(filePart <= numParts) {
			//partition file
			String partName = file.getParent() + "/" + prefix + Integer.toString(filePart);
			File partFile = new File(partName);
			
			//streams for output file
			FileWriter fstream = null;
			BufferedWriter out = null;
			try {
				fstream = new FileWriter(partFile, true);
				out = new BufferedWriter(fstream);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			int lineNum = 0;
			
			//write until linePerPartition or end of file
			try {	
				String aLine;
				while ((lineNum < linePerPartition) && ((aLine = in.readLine()) != null)) {
					out.write(aLine);
					out.newLine();
					lineNum++;
				}
				out.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			//increment partition number
			filePart++;
		}
		
		try {
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * numLines() returns no. of lines for a file
	 */
	static int numLines(File file) {
		//file and line reader
		FileReader fr = null;
		LineNumberReader lnr = null;
		
		int n = 0; //no of lines
		
		//read each line and increment no. of lines counter
		try {
			fr = new FileReader(file);
			lnr = new LineNumberReader(fr);
			
			while(lnr.readLine()!=null) {
				n++;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			fr.close();
			lnr.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return n;
	}
	
	/*
	 * send partNum (partition number) of fileName to nodNum, for a jobId
	 * 
	 * NOTE: uses sendToNode() for transferring the file over the network
	 */
	static void sendPartitionToNode(String fileName, int partNum, int nodeNum, int jobID) {
		//check if file is present in the DFS
		if(filePresent(fileName) == 0) {
			System.out.println(fileName + " does not exist in the DFS!");
			return;
		}
		
		//get formatted of fileName
		fileName = getFormattedFileName(fileName);
		
		//find if partition is in nodeNum
		int present = checkFilePartInNode(fileName, partNum, nodeNum);
		//if part not present, send to nodeNum using Network
		if(present == 0) {
			String partitionFileName = "./root/" + Integer.toString(nodeNum) + "/" + fileName 
					+ Integer.toString(partNum);
			File partitionFile = new File(partitionFileName);
			
			sendToNode(nodeNum, partitionFile);
		}
	}
	
	/*
	 * sendMapOutputToNode() sends the map output for jobID in mapNode to
	 * the reduceNode using the network
	 * 
	 * NOTE: uses sendToNode for transferring the files through the network
	 */
	static void sendMapOutputToNode(int jobId, int mapNode, int partitionNum, 
			int reduceNode, int reducerNum, String inputDir) {
		//get formatted file name for the inputDir of the MapRedice job
		File dummyFile = new File(inputDir);
		String formattedName = dummyFile.getName();
		
		//1. get the jobId's map partition in the mapNode
		//2. send this to the reduceNode with the correct formatting of the fileName
		String mapOutputFileName = "./root/" + Integer.toString(mapNode) + "/" + formattedName + 
				"_" + Integer.toString(jobId) + "_" + Integer.toString(partitionNum) + "out"
				+ Integer.toString(reducerNum);
		File mapOutputFile = new File(mapOutputFileName);
		
		sendToNode(reduceNode, mapOutputFile);
	}
	
	/*
	 * sendToNode sends a file to a nodeNum (TaskTracker) on the cluster
	 * using network sockets.
	 */
	static void sendToNode(int nodeNum, File file) {
		/*
		 * connect to the TaskTracker using network info
		 * from nodeInfo for a nodeNum
		 */
		Socket connection = null;
		try {
			connection = new Socket(InetAddress.getByName(nodeInfo.get(nodeNum).getIPAddress()), 
					nodeInfo.get(nodeNum).getServPort());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//write file to TaskTracer using CopyFile command
		try {
			//output stream for file
			ObjectOutputStream oos = new ObjectOutputStream(connection.getOutputStream());
			oos.flush();
			ObjectInputStream ois = new ObjectInputStream(connection.getInputStream());
			
			//input buffered stream for file
			byte[] buffer = new byte[1024];
			OutputStream out = connection.getOutputStream();
			BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
			
			//send CopyFile command and destFileName to the TaskTracker
			oos.writeObject("CopyFile");
			String destFileName = "./root/" + Integer.toString(nodeNum) + "/" + file.getName(); 
			oos.writeObject(destFileName);
			
			int count;
			
			//write until file reaches end, i.e. no. of bytes written = 0
			while ((count = in.read(buffer)) > 0) {
			     out.write(buffer, 0, count);
			     out.flush();
			}
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/* 
	 * sendToNodeFolder() sends a file to nodeNum's dirName using network
	 * NOTE: simialr to sendToNode() but allows for specific 
	 * 		 dirName inside the node
	 */
	static void sendToNodeFolder(int nodeNum, File file, String dirName) {
		//create dirPath if doesn't exist in nodeNum
		String dirPath = "./root/" + Integer.toString(nodeNum) + "/" + dirName;
		File dir = new File(dirPath);
		
		//create dir if missing
		if(!dir.exists()) {
			dir.mkdir();
		}
		
		//socket connection to nodeNum
		Socket connection = null;
		try {
			connection = new Socket(InetAddress.getByName(nodeInfo.get(nodeNum).getIPAddress()), 
					nodeInfo.get(nodeNum).getServPort());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//send file to TaskTracker's dirName using CopyFile command
		try {
			//output stream for file
			ObjectOutputStream oos = new ObjectOutputStream(connection.getOutputStream());
			oos.flush();
			ObjectInputStream ois = new ObjectInputStream(connection.getInputStream());
			
			//input buffered stream for file
			byte[] buffer = new byte[1024];
			OutputStream out = connection.getOutputStream();
			BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
			
			//send CopyFile command and destFileName to the TaskTracker
			oos.writeObject("CopyFile");
			String destFileName = dirPath + "/" + file.getName(); 
			oos.writeObject(destFileName);
			
			int count;
			
			//write until file reaches end, i.e. no. of bytes written = 0
			while ((count = in.read(buffer)) > 0) {
			     out.write(buffer, 0, count);
			     out.flush();
			}
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * sendToJobTrackerFolder sends a file to Job Tracker's dirName 
	 * using network
	 * NOTE: simialr to sendToNode() but allows for specific 
	 * 		 dirName inside the JobTracker
	 */
	static void sendToJobTrackerFolder(File file, String dirName) {
		
		//create dirPath if doesn't exist in nodeNum
		//String dirPath = "./root/" + dirName;
		String dirPath = dirName;
		File dir = new File(dirPath);
		
		//create dir if missing
		if(!dir.exists()) {
			dir.mkdir();
		}
		
		//socket connection to the JobTracker
		Socket connection = null;
		try {
			connection = new Socket(jobTrackerIP, jobTrackerPort);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		//send file to TaskTracker's dirName using CopyFile command
		try {
			//output stream for file
			ObjectOutputStream oos = new ObjectOutputStream(connection.getOutputStream());
			oos.flush();
			ObjectInputStream ois = new ObjectInputStream(connection.getInputStream());
			
			//input buffered stream for file
			byte[] buffer = new byte[1024];
			OutputStream out = connection.getOutputStream();
			BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
			
			//send CopyFile command and destFileName to the TaskTracker
			oos.writeObject("copyFile");
			String destFileName = dirPath + "/" + file.getName(); 
			oos.writeObject(destFileName);
			
			int count;
			
			//write until file reaches end, i.e. no. of bytes written = 0
			while ((count = in.read(buffer)) > 0) {
			     out.write(buffer, 0, count);
			     out.flush();
			}
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/*
	 * getFileLength() returns fileLength of fileName
	 * 1. if in DFS, returns the true fileLength
	 * 2. if not present, returns -1
	 */
	static int getFileLength(String fileName) {
		if(filePresent(fileName) == 0) {
			return -1;
		}
		
		fileName = getFormattedFileName(fileName);
		
		return fileTable.get(fileName).getFileLength();
	}
	
	/*
	 * getFormattedFileName returns the formatted file name,
	 * 
	 * i.e. returns the fileName without any formatting for
	 * file separators
	 */
	static String getFormattedFileName(String fileName) {
		File dummyFile = new File(fileName);
		return dummyFile.getName();
	}
	
	/*
	 * getNumParts() returns the no. of partitions of fileName's
	 * entry in the DFS
	 */
	static int getNumParts(String fileName) {
		if(filePresent(fileName) == 0) {
			return 0;
		}
		
		fileName = getFormattedFileName(fileName);
		
		return fileTable.get(fileName).getNumParts();
	}
	
	/*
	 * checkFilePartInNode() returns:
	 * 1. 1 if the fileName's (file) partNum (partition) is present
	 *    in the nodeNum (node)
	 * 2. 0 if not present
	 */
	static int checkFilePartInNode(String fileName, int partNum, int nodeNum) {
		if(filePresent(fileName) == 0) {
			return 0;
		}
		
		fileName = getFormattedFileName(fileName);
		
		int present = 0;
		
		for(int i=0; i<(fileTable.get(fileName).getRFactor()); i++) {
			if(fileTable.get(fileName).getEntry(partNum-1, i) == nodeNum) {
				present = 1;
				break;
			}
		}
		
		return present;
	}
	
	/*
	 * addOutputFile() adds the output fileName's info in the fieTable
	 * after the reduce phase and returns
	 * 1. 1 if fileName is already present in the FileSystem/fileTable
	 * 2. 0 if added succesfully
	 */
	static int addOutputFile(String fileName, int numReducers) {
		//check if file is already present
		if(filePresent(fileName)==1) {
			return 1;
		}
		
		/*
		 * create new FileEntry for the file, with no. of partitions as
		 * numReducers(number of reducers)
		 */
		FileEntry fileEntry = new FileEntry(fileName, numReducers, 1, -1);
		fileTable.put(fileName, fileEntry);
		
		//initialize all parts to 0 node
		for(int i=0; i<numReducers; i++) {
			fileTable.get(fileName).editList(i, 0, 0);
		}
		
		return 0;
	}
	
	/*
	 * addOutputFilePart() adds the entry for a specific partition in 
	 * the fileSystem for a fileName
	 */
	static void addOutputFilePart(String fileName, int partNum, int nodeNum) {
		fileTable.get(fileName).editList(partNum-1, 0, nodeNum);
	}
	
	/*
	 * getFileEntry() returns the FileEntry object for a fileName, as
	 * stored in the fileTable
	 */
	static FileEntry getFileEntry(String fileName) {
		FileEntry fileEntry = null;
		
		fileName = getFormattedFileName(fileName);
		if(filePresent(fileName) == 1) {
			fileEntry = fileTable.get(fileName);
		}
		
		return fileEntry;
	}
}