import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

/*
 * class for storing TaskTracker's info
 */
public class TaskTrackerInfo implements Serializable {
	String ip; //ip for TsakTracker
	int pollPort; //polling port for TaskTracker
	int servPort; //server service port for TaskTracker
	int nodeNum; //nodeNum of the TaskTracker
	double load; //load of the TaskTracker
	
	//constructor for initializing the fields
	TaskTrackerInfo(String ip, int pollPort, int servPort, int nodeNum) {
		this.ip = ip;
		this.pollPort = pollPort;
		this.servPort = servPort;
		this.nodeNum = nodeNum;
		this.load = 0;
	}
	
	//add load to the TaskTracker
	void addLoad(double load) {
		this.load += load;
	}
	
	//remove load from the TaskTracker
	void removeLoad(double load) {
		this.load -= load;
	}
	
	//returns load of a TaskTracker
	double getLoad() {
		return this.load;
	}
	
	//returns the IP address of the TaskTracker
	String getIPAddress() {
		return ip;
	}
	
	//returns the polling port no. of the TaskTracker
	int getPollPort() { 
		return pollPort;
	}
	
	//returns the server port of the TaskTracker
	int getServPort() {
		return servPort;
	}
	
	//returns the NodeNum of the TaskTracker
	int getNodeNum() {
		return nodeNum;
	}
	
	//prints the TaskTrackerInfo fields
	void printInfo() {
		System.out.println("Node" + nodeNum + "- " + ip + ":" + servPort + "\tPoll Port: " + pollPort);
	}
}