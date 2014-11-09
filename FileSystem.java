import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

public class FileSystem {
	//fileSystem for Directory names
	static Hashtable<String, FileEntry> fileTable = new Hashtable<String, FileEntry>();
	//list of active nodes in cluster
	static List<Integer> nodeList = new ArrayList<Integer>();
	//nodeInfo for every node in the cluster
	static Hashtable<Integer,TaskTrackerInfo> nodeInfo = new Hashtable<Integer,TaskTrackerInfo>();
	
	//constructor which initializes the fileTable
	FileSystem() {
		createRoot();
	}
	
	//sets up a root directry for the DFS
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

            if(dir.mkdir()) {
            	System.out.println("New Root directory for DFS created");
            }
            else {
            	System.out.println("New Root directory for DFS could not be created. Exiting!");
                System.exit(1);
            }
        }
	}
	
	//recursive delete
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
	
	/*Create directory for new TaskTracker*/
	static void addNode(TaskTrackerInfo taskTrackerInfo) {
		//add to nodeList and nodeInfo
		int nodeNum = taskTrackerInfo.getNodeNum();
				
		nodeList.add(nodeNum);
		try {
			nodeInfo.put(nodeNum, taskTrackerInfo);
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		String dirName = "./root/" + Integer.toString(nodeNum);
		File dir = new File(dirName);
		
		if(dir.exists()) {
			delete(dir);
			System.out.println("Existing DFS Directory for node " + nodeNum + " deleted.");
		}
    	
		if(dir.mkdir()) {
			System.out.println("New DFS Directory for node " + nodeNum + " created");
	    }
        else {
        	System.out.println("New DFS Directory for node " + nodeNum + " could not be created. Exiting");
        	System.exit(1);
        }
	}
	
	//removes a node from the FileSystem
	static void removeNode(int nodeNum) {
		//remove from nodeList
		Integer nodeToRemove = new Integer(nodeNum);
		nodeList.remove(nodeToRemove);
		
		//remove from nodeInfo
		try {
			nodeInfo.remove(nodeNum);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		String dirName = "./root/" + Integer.toString(nodeNum);
		File dir = new File(dirName);
		
		if(dir.exists()) {
			delete(dir);
			System.out.println("DFS directory for failed Node " + nodeNum + " removed");
		}
		
		deleteNodeFiles(nodeNum);
	}
	
	//deletes all file replicas for a nodeNum
	static void deleteNodeFiles(int nodeNum) {
		Set<String> keys = fileTable.keySet();
        for(String key: keys){
            FileEntry fileEntry = fileTable.get(key);
            
            for(int i=0; i<fileEntry.getRFactor(); i++) {
        		if(fileEntry.getEntry(i) == nodeNum) {
        			fileEntry.editList(i,-1); //replica missing due to failure
        		}
            }
        }
	}
	
	//adds a file(directory) to the fileTable
	static int addFile(String fileName) {
		File srcDir = new File(fileName);

		if(!srcDir.exists()) {
			return 1;
		} else if(!srcDir.isDirectory()) {
			return 2;
		}
		
		if(fileTable.containsKey(srcDir.getName())) {
			return 3;
		} else {
			//temp copy to root
			String dest = "./root/" + srcDir.getName();
			File destDir = new File(dest);
			
			copyDir(srcDir,destDir);
			
			//add from temp to fileTable
			addEntry(destDir);
			
			//remove temp copy
			//delete(destDir);
		}
		
		return 0;
	}
	
	//delete fileName from cluster and fileTable
	static int deleteFile(String fileName) {
		if(!fileTable.containsKey(fileName)) {
			return 1;
		}
		
		int rFactor = fileTable.get(fileName).getRFactor();
		
		//delete replicas
		for(int i=0; i<rFactor; i++) {
			int nodeNum = fileTable.get(fileName).getEntry(i);
			
			if(nodeNum>0) {
				String deleteFileName = "./root/" + Integer.toString(nodeNum) + "/" + fileName;
				File delFile = new File(deleteFileName);
				
				delete(delFile);
			}
		}
		
		//delete from fileTables
		fileTable.remove(fileName);
		
		return 0;
	}
	
	//copies a src Directory to a new dest Dir
	static void copyDir(File src, File dest) {
		if(dest.exists()) {
			//Send some message to MapReduce client
			System.out.println(dest.getName() + " already exists in the DFS");
		} else {
			if(src.isDirectory()) {
				dest.mkdir();
				System.out.println(dest.getName() + " created");

				String files[] = src.list();

				for(String file: files) {
					File srcFile = new File(src,file);
					File destFile = new File(dest,file);
					
					copyDir(srcFile,destFile);
				}
			} else {
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
	
	static void addEntry(File destDir) {
		if(!destDir.exists()) {
			System.out.println(destDir.getName() + " couldn't be found in /root");
			return;
		}
		
		String fileNames[] = destDir.list();
		int numFiles = 0;
		
		//number of files
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
		
		//int rFactor = 3; // number of replicas created
		int rFactor = 1; // for testing purposes
		
		/*
		 * Change name of mergedFile to original directory name
		 */
		//splitFiles(mergedFile, destDir.getName(), numParts);
		String newFileName = "./root/" + destDir.getName() + "/" + destDir.getName();
		File newFile = new File(newFileName);
		boolean mergeStatus = mergedFile.renameTo(newFile);
		if(mergeStatus) {
			System.out.println("Files in " + destDir.getName() + " merged successfully");
		} else {
			System.out.println("Files in " + destDir.getName() + " could not be merged successfully");
		}
		
		//check if nodes < replication factor
		if(nodeList.size() < rFactor) {
			System.out.println("Number of Nodes is less than the replication factor of " + rFactor);
			System.out.println(destDir.getName() + " could not be added");
		} else {
			//add to fileTable
			FileEntry fileEntry = new FileEntry(destDir.getName(), rFactor);
			fileTable.put(destDir.getName(), fileEntry);
			
			//randomNumbers list to decide nodes for replicas
			ArrayList<Integer> randomNumbers = new ArrayList<Integer>();
			for(int i = 0; i<(nodeList.size()); i++) {
				randomNumbers.add(i+1);
			}
			Collections.shuffle(randomNumbers);
			
			for(int i=0; i<rFactor; i++) {
				//partionFileName and File object
				String fileName = destDir.getAbsolutePath() + "/" + destDir.getName();
				File file = new File(fileName);
				
				//make replicaCopy with name appended
				//String replicaCopyName = fileName + Integer.toString(i+1);
				//File replicaCopy = new File(replicaCopyName);
				//copyDir(file, replicaCopy);				
				
				//send replicas to nodes
				int node = randomNumbers.get(i);
				System.out.println("Copy " + (i+1) + " => " + "Node " + node);
				sendToNode(node, file);
				fileTable.get(destDir.getName()).editList(i, node);
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
	
	static void mergeFiles(File[] files, File mergedFile) {
		FileWriter fstream = null;
		BufferedWriter out = null;
		try {
			fstream = new FileWriter(mergedFile, true);
			out = new BufferedWriter(fstream);
		} catch (Exception e) {
			e.printStackTrace();
		}
 
		for (File f : files) {
			System.out.println("merging: " + f.getName());
			FileInputStream fis;
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
 
		try {
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//returns 1 if present in FileSystem, else 0
	static int filePresent(String fileName) {
		int result = 0;
		
		File file = new File(fileName);
		String formattedFileName = file.getName();
		
		if(fileTable.containsKey(formattedFileName)) {
			result = 1;
		}
		
		return result;
	}
	
	//split file into partitions at master
	static void splitToPartitions(String fileName, int numParts) {
		if(filePresent(fileName) == 0) {
			System.out.println(fileName + " not in DFS!");
			return;
		}
		
		File dummyFile = new File(fileName);
		String formattedFileName = dummyFile.getName();
		
		formattedFileName = "./root/" + formattedFileName + "/" + formattedFileName;
		File file = new File(formattedFileName);
		
		splitFiles(file, file.getName(), numParts);
		
		System.out.println("Files partitioned for " + formattedFileName);
	}
	
	static void splitFiles(File file, String prefix, int numParts) {
		//lines per partition
		int linePerPartition = (int)Math.ceil((float)numLines(file)/numParts);
		
		int filePart = 1; //current partition number
		
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
			
			filePart++; //increment partition number
		}
		
		try {
			in.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	static int numLines(File file) {
		FileReader fr = null;
		LineNumberReader lnr = null;
		
		int n = 0; //no of lines
		
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
	
	//send partNum (partition number) of fileName to nodNum
	static void sendPartitionToNode(String fileName, int partNum, int nodeNum) {
		if(filePresent(fileName) == 0) {
			System.out.println(fileName + " does not exist in the DFS!");
			return;
		}
		
		//check formatting of fileName
		File dummyFile = new File(fileName);
		String formattedFileName = dummyFile.getName();
		
		String partitionFileName = "./root/" + formattedFileName + "/" + formattedFileName 
				+ Integer.toString(partNum);
		File partitionFile = new File(partitionFileName);
		
		if(!partitionFile.exists()) {
			System.out.println("Partition " + partNum + " does not exist for " + fileName);
			return;
		}
		
		sendToNode(nodeNum, partitionFile);
	}
	
	//send a file to nodeNum using network
	static void sendToNode(int nodeNum, File file) {
		
		Socket connection = null;
		try {
			connection = new Socket(InetAddress.getByName(nodeInfo.get(nodeNum).getIPAddress()), 
					nodeInfo.get(nodeNum).getServPort());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try {
			ObjectOutputStream oos = new ObjectOutputStream(connection.getOutputStream());
			oos.flush();
			ObjectInputStream ois = new ObjectInputStream(connection.getInputStream());
			
			byte[] buffer = new byte[1024];
			OutputStream out = connection.getOutputStream();
			BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
			
			oos.writeObject("CopyFile");
			String destFileName = "./root/" + Integer.toString(nodeNum) + "/" + file.getName(); 
			oos.writeObject(destFileName);
			
			int count;
			
			while ((count = in.read(buffer)) > 0) {
			     out.write(buffer, 0, count);
			     out.flush();
			}
			out.close();
			
			//System.out.println((String)ois.readObject());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	//return fileLength of fileName if in DFS, else return -1
	static int getFileLength(String fileName) {
		if(filePresent(fileName) == 0) {
			return -1;
		}
		
		fileName = getFormattedFileName(fileName);
		fileName = "./root/" + fileName + "/" + fileName;
		
		File file = new File(fileName);
		return (int)file.length();
	}
	
	static String getFormattedFileName(String fileName) {
		File dummyFile = new File(fileName);
		return dummyFile.getName();
	}
}