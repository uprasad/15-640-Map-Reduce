import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

/*
 * class for storing details of a Task in a Job
 */
public class TaskDetails implements Serializable{
	
	/*
	 * Task status
	 * 0 - Not started
	 * 1 - Submitted
	 * 2 - Success
	 * 3 - Error
	 * 4 - Failed once, Retrying
	 * 5 - Failed twice, Aborted
	 */
	private int status;
	
	private int partition; // partition number of the input file
	private int nodeNum; // unique node number associated with the TaskTracker
	private double load; // the amount of input that is being processed on this machine in total
	
	int attempt = 0; // attempt of run
	
	//constructor for initializing the fields
	TaskDetails(int partition, int nodeNum, double load) {
		this.partition = partition;
		this.nodeNum = nodeNum;
		this.status = 0;
		this.load = load;
		
		attempt++;
	}
	
	//returns the partition number for the task
	int getPartition() {
		return this.partition;
	}
	
	//returns the status of the task
	int getStatus() {
		return this.status;
	}
	
	//returns the node number of the task
	int getNodeNum() {
		return this.nodeNum;
	}
	
	//returns the load of the task
	double getLoad() {
		return this.load;
	}
	
	//set the status of the task
	void setStatus(int newStatus) {
		this.status = newStatus;
	}
	
	//changes the node of run for the task
	void changeNodeNum(int nodeNum) {
		this.nodeNum = nodeNum;
	}
	
	//add attempt count of the task
	void incrementAttempt() {
		attempt++;
	}
	
	//returns the attempt count for the task
	int getAttempt() {
		return this.attempt;
	}
	
	//returns the statusMessage of the Task
	String getStatusMessage() {
		if(status == 0) {
			return "Task Not Started";
		} else if(status == 1) {
			return "Task Submitted";
		} else if(status == 2) {
			return "Task completed successfully";
		} else if(status == 3) {
			return "Error in Task, exitvalue() 1. Aborted.";
		} else if(status == 4) {
			return "Failed once, Retyring!";
		} else if(status == 5) {
			return "Failed twice. Aborted.";
		} else {
			return "NA";
		}
	}
}