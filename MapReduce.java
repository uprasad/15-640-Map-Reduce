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
		System.out.println("Enter input directory");
		
		String inputDir = null;
		Scanner in = new Scanner(System.in);
		inputDir = in.nextLine();
		
		File fileObject = new File(inputDir);
		inputDir = fileObject.getName();
		
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
			String ack = (String)ois.readObject();
			
			if (ack.equals("NotDir")) {
				System.out.println("Not a directory!");
			} else if (ack.equals("NotExist")) {
				System.out.println("No such directory exists!");
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
