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
	 */
	private int status;
	private int partition;
	private int nodeNum;
	private double load;
	
	TaskDetails(int partition, int nodeNum, double load) {
		this.partition = partition;
		this.nodeNum = nodeNum;
		this.status = 0;
		this.load = load;
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
	
	String getStatusMessage() {
		if(status == 0) {
			return "Task Not Started";
		} else if(status == 1) {
			return "Task Submitted";
		} else if(status == 2) {
			return "Task completed successfully";
		} else if(status == 3) {
			return "Error in task. Aborted.";
		} else {
			return "NA";
		}
	}
}