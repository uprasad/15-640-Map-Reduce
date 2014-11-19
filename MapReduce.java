import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

/*UserInput for user interface*/
class UserInput implements Runnable {
	/* Thread to accept user inputs*/
	public void run() {
		int choice = 0;
		do {
			MapReduce.showMenu();
			Scanner inputScanner = new Scanner(System.in);
			
			try {
				choice = inputScanner.nextInt();
			} catch (Exception e) {
				System.out.println("Please input an integer.");
				continue;
			}
			
			switch(choice) {
			case 0:
				System.exit(0);
			case 1:
				MapReduce.printTaskTrackerList();
				break;
			case 2:
				MapReduce.copyData();
				break;
			case 3:
				MapReduce.deleteData();
				break;
			case 4:
				MapReduce.startNewJob();
				break;
			case 5:
				MapReduce.jobInfo();
				break;
			case 6:
				MapReduce.getOutput();
				break;
			default:
				System.out.println("Invalid choice. Try again.");
				choice = -1;
				break;
			}
		} while (choice != 0);
	}
	
}

public class MapReduce {
	
	static String jobTrackerIP = null;
	static int jobTrackerPort;
	
	/*Menu for UserInput*/
	static void showMenu() {
		System.out.println();
		System.out.println("************************");
		System.out.println("0. Exit");
		System.out.println("1. List Task Trackers");
		System.out.println("2. Copy Directory to DFS");
		System.out.println("3. Delete Directory in DFS");
		System.out.println("4. Start a new job");
		System.out.println("5. View Job Details");
		System.out.println("6. View Output Files");
		System.out.println("************************");
		System.out.println();
	}
	
	public static void main(String args[]) {
		if (args.length < 2) {
			System.out.println("Usage: java MapReduce <JobTracker IP> <JobTracker port>");
			System.exit(1);
		}
		
		jobTrackerIP = args[0];
		jobTrackerPort = Integer.parseInt(args[1]);
		
		/*Start UserInput Thread*/
		Runnable userRunnable = new UserInput();
		Thread userThread = new Thread(userRunnable);
		userThread.start();
		
		// Do other tasks here...
	}
	
	/*Requests the JobTracker for list of data nodes*/
	static void printTaskTrackerList() {
		Socket socket = null;
		ObjectInputStream ois = null;
		ObjectOutputStream oos = null;
		
		/*Connect to JobTracker*/
		try {
			socket = new Socket(jobTrackerIP, jobTrackerPort);
			oos = new ObjectOutputStream(socket.getOutputStream());
			oos.flush();
			ois = new ObjectInputStream(socket.getInputStream());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/*Get list from JobTracker*/
		ArrayList<TaskTrackerInfo> taskTrackerInfo = null;
		try {
			oos.writeObject("ListTaskTrackers");
			taskTrackerInfo = (ArrayList<TaskTrackerInfo>)ois.readObject();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/*Iterate and print all data nodes*/
		Iterator<TaskTrackerInfo> iter = taskTrackerInfo.iterator();
		TaskTrackerInfo tti = null;
		while(iter.hasNext()) {
			tti = iter.next();
			tti.printInfo();
		}
	}
	
	static void jobInfo() {
		System.out.print("Enter jobID:");
		
		//get jobId from user
		String jID;
		Scanner in = new Scanner(System.in);
		jID = in.nextLine();
		
		Integer jobId = Integer.parseInt(jID); 
		
		Socket socket = null;
		ObjectInputStream ois = null;
		ObjectOutputStream oos = null;
		
		/*Connect to JobTracker*/
		try {
			socket = new Socket(jobTrackerIP, jobTrackerPort);
			oos = new ObjectOutputStream(socket.getOutputStream());
			oos.flush();
			ois = new ObjectInputStream(socket.getInputStream());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/*Get jobInfo from JobTracker*/
		Job jobInfo = null;
		List<TaskDetails> mapList = null;
		List<TaskDetails> reduceList = null;
		try {
			oos.writeObject("JobInfo");
			oos.writeObject(jobId);
			jobInfo = (Job)ois.readObject();
			mapList = (List<TaskDetails>)ois.readObject();
			reduceList = (List<TaskDetails>)ois.readObject();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if(jobInfo == null) {
			System.out.println("Job with jobID " + jobId + " not found!");
			return;
		}
		
		/*Iterate and print all task details*/
		int numMappers = jobInfo.getNumMappers();
		int numReducers = jobInfo.getNumReducers();
		
		String inputDir = jobInfo.getInputDir();
		String outputDir = jobInfo.getOutputDir();
		
		System.out.println("\nJob " + jobId);
		System.out.println("NumMappers:" + numMappers + "\tNumReducers:" + numReducers);
		System.out.println("InputDir:" + inputDir + "\tOutputDir:" + outputDir);
		System.out.println("----------------");
		System.out.println("Map Phase");
		
		if(mapList != null && mapList.size()==numMappers) {		
			for(int i=0; i<numMappers; i++) {
				int nodeNum = mapList.get(i).getNodeNum();
				String statusMessage = mapList.get(i).getStatusMessage(); 
				
				System.out.println("Mapper " + (i+1) + "\tnodeNum:" + nodeNum + "\tStatus:" + statusMessage);
			}
		}
		
		System.out.println("----------------");
		System.out.println("Reduce Phase");
		
		if(reduceList != null && reduceList.size()==numReducers) {
			for(int i=0; i<numReducers; i++) {
				int nodeNum = reduceList.get(i).getNodeNum();
				String statusMessage = reduceList.get(i).getStatusMessage(); 
				
				System.out.println("Reducer " + (i+1) + "\tnodeNum:" + nodeNum + "\tStatus:" + statusMessage);
			}
		}
	}
	
	static void getOutput() {
		System.out.print("Enter Output Directory Name:");
		
		//get jobId from user
		String outputDir;
		Scanner in = new Scanner(System.in);
		outputDir = in.nextLine();
		
		Socket socket = null;
		ObjectInputStream ois = null;
		ObjectOutputStream oos = null;
		
		/*Connect to JobTracker*/
		try {
			socket = new Socket(jobTrackerIP, jobTrackerPort);
			oos = new ObjectOutputStream(socket.getOutputStream());
			oos.flush();
			ois = new ObjectInputStream(socket.getInputStream());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/*Get jobInfo from JobTracker*/
		FileEntry outputEntry = null;
		try {
			oos.writeObject("GetOutput");
			oos.writeObject(outputDir);
			outputEntry = (FileEntry)ois.readObject();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		if(outputEntry == null) {
			System.out.println("Output Directory " + outputDir + " not found on the DFS!");
			return;
		}
		
		/*Iterate and print all task details*/
		int numParts = outputEntry.getNumParts();
		String outputFileName = outputEntry.getFileName();
		
		System.out.println("----------------");
		System.out.println("Output Directory: " + outputFileName);
		
		for(int i=0; i<numParts; i++) {
			int nodeNum = outputEntry.getEntry(i, 0);
			String partPath = "./root/" + Integer.toString(nodeNum) + "/" +
					outputFileName + Integer.toString(i+1);
			System.out.println("File " + (i+1) + " path: " + partPath);
		}
	}

	static void copyData() {
		System.out.print("Enter source directory:");
		
		String src;
		Scanner in = new Scanner(System.in);
		src = in.nextLine();

		Socket socket = null;
		ObjectInputStream ois = null;
        ObjectOutputStream oos = null;

        /*Connect to JobTracker*/
        try {
                socket = new Socket(jobTrackerIP, jobTrackerPort);
                oos = new ObjectOutputStream(socket.getOutputStream());
                oos.flush();
                ois = new ObjectInputStream(socket.getInputStream());
        } catch (Exception e) {
                e.printStackTrace();
        }

		try {
			oos.writeObject("copy");
			oos.writeObject(src);			

			String command = (String)ois.readObject();
			if (command.equals("notFound")) {
				System.out.println(src + " not found");
			} else if (command.equals("notDir")) {
				System.out.println(src + " is not a directory");
			} else if (command.equals("OK")) {
				System.out.println(src + " successfully copied into DFS");
			} else if (command.equals("duplicate")) {
				System.out.println(src + " already present in DFS. Delete previous copy first!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	static void deleteData() {
		System.out.print("Enter directory:");
		
		String src;
		Scanner in = new Scanner(System.in);
		src = in.nextLine();
		
		File fileObject = new File(src);
		src = fileObject.getName();

		Socket socket = null;
		ObjectInputStream ois = null;
        ObjectOutputStream oos = null;

        /*Connect to JobTracker*/
        try {
                socket = new Socket(jobTrackerIP, jobTrackerPort);
                oos = new ObjectOutputStream(socket.getOutputStream());
                oos.flush();
                ois = new ObjectInputStream(socket.getInputStream());
        } catch (Exception e) {
                e.printStackTrace();
        }

		try {
			oos.writeObject("delete");
			oos.writeObject(src);	

			String command = (String)ois.readObject();
			if (command.equals("notFound")) {
				System.out.println(src + " not found");
			} else if (command.equals("OK")) {
				System.out.println(src + " successfully deleted from DFS");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	static void startNewJob() {
		System.out.println("Enter config file path");
		
		String configPath = null;
		Scanner in = new Scanner(System.in);
		configPath = in.nextLine();
		
		File configFileObject = new File(configPath);
		//inputDir = fileObject.getName();
		
		if(!configFileObject.exists()) {
			System.out.println("The config file " + configPath + " does not exist. Try again!");
			return;
		}
		
		//load properties from config file
		Properties properties = new Properties();
		try {
            FileInputStream configFIS = new FileInputStream(configFileObject);
            properties.load(configFIS);
            configFIS.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
		
		//read INPUT_DIR
		if(!properties.containsKey("INPUT_DIR")) {
			System.out.println("INPUT_DIR is missing in the config file. Try again!");
			return;
		}
		
		String inputDir = (String)properties.get("INPUT_DIR");
		
		if(inputDir == null) {
			System.out.println("INPUT_DIR entry is missing in the config file. Try again!");
			return;
		}
		
		//read OUTPUT_DIR
		if(!properties.containsKey("OUTPUT_DIR")) {
			System.out.println("OUTPUT_DIR is missing in the config file. Try again!");
			return;
		}
		
		String outputDir = (String)properties.get("OUTPUT_DIR");
		
		if(outputDir == null) {
			System.out.println("OUTPUT_DIR entry is missing in the config file. Try again!");
			return;
		}
		
		//read numReducers
		Integer numReducers;
		if(!properties.containsKey("NUM_REDUCERS")) {
			System.out.println("NUM_REDUCERS is missing in the config file. Try again!");
			return;
		}
		
		numReducers = Integer.parseInt((String)properties.get("NUM_REDUCERS"));
		
		if(numReducers == null) {
			System.out.println("NUM_REDUCERS entry is missing in the config file. Try again!");
			return;
		}
		
		Socket socket = null;
		ObjectInputStream ois = null;
        ObjectOutputStream oos = null;

        /*Connect to JobTracker*/
        try {
                socket = new Socket(jobTrackerIP, jobTrackerPort);
                oos = new ObjectOutputStream(socket.getOutputStream());
                oos.flush();
                ois = new ObjectInputStream(socket.getInputStream());
        } catch (Exception e) {
                e.printStackTrace();
        }
        
        try {
			oos.writeObject("NewJob");
			oos.writeObject(inputDir);
			oos.writeObject(outputDir);
			oos.writeObject(numReducers);
			String ack = (String)ois.readObject();
			
			if (ack.equals("NotDir")) {
				System.out.println(inputDir + " is not a directory!");
			} else if (ack.equals("NotExist")) {
				System.out.println(inputDir + " does not exist in the DFS!");
			} else if (ack.equals("OutDuplicate")) {
				System.out.println(outputDir + " already exists in the DFS. Try another name!");
			} else if (ack.equals("AckDir")) {
				
				System.out.println("Name of .jar file with compiled .class files of mapper and reducer");
				String mapredJar = in.nextLine();
				File file = new File(mapredJar);
				long length = file.length();
				
				if (length > Integer.MAX_VALUE) {
					System.out.println("File is too large.");
				}
				
				if (!file.exists()) {
					System.out.println("The file does not exist.");
				} else if (!file.getName().substring(file.getName().length() - 3).equals("jar")) {
					System.out.println("It is not a .jar file");
				} else {
					// Send the .jar file
					byte[] bytes = new byte[(int)length];
					FileInputStream fis = new FileInputStream(file);
					BufferedInputStream bis = new BufferedInputStream(fis);
					BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());
					
					int count;
					
					while ((count = bis.read(bytes)) > 0) {
				        out.write(bytes, 0, count);
				    }
	
				    out.flush();
				    out.close();
				    fis.close();
				    bis.close();
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
