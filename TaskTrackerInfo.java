import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

public class TaskTrackerInfo implements Serializable {
	String ip;
	int pollPort;
	int servPort;
	int nodeNum;
	double load;
	
	TaskTrackerInfo(String ip, int pollPort, int servPort, int nodeNum) {
		this.ip = ip;
		this.pollPort = pollPort;
		this.servPort = servPort;
		this.nodeNum = nodeNum;
		this.load = 0;
	}
	
	void addLoad(double load) {
		this.load += load;
	}
	
	void removeLoad(double load) {
		this.load -= load;
	}
	
	double getLoad() {
		return this.load;
	}
	
	String getIPAddress() {
		return ip;
	}
	
	int getPollPort() { 
		return pollPort;
	}
	
	int getServPort() {
		return servPort;
	}
	
	int getNodeNum() {
		return nodeNum;
	}
	
	void printInfo() {
		System.out.println("Node" + nodeNum + "- " + ip + ":" + servPort + "\tPoll Port: " + pollPort);
	}
}