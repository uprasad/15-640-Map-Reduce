import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

public class TaskDetails implements Serializable{
	
	/*
	 * status
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
	
	int attempt = 0;
	
	TaskDetails(int partition, int nodeNum, double load) {
		this.partition = partition;
		this.nodeNum = nodeNum;
		this.status = 0;
		this.load = load;
		
		attempt++;
	}
	
	int getPartition() {
		return this.partition;
	}
	
	int getStatus() {
		return this.status;
	}
	
	int getNodeNum() {
		return this.nodeNum;
	}
	
	double getLoad() {
		return this.load;
	}
	
	void setStatus(int newStatus) {
		this.status = newStatus;
	}
	
	void changeNodeNum(int nodeNum) {
		this.nodeNum = nodeNum;
	}
	
	void incrementAttempt() {
		attempt++;
	}
	
	int getAttempt() {
		return this.attempt;
	}
	
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