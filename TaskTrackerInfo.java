import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

public class TaskTrackerInfo implements Serializable {
	String ip;
	int pollPort;
	int servPort;
	int nodeNum;
	
	TaskTrackerInfo(String ip, int pollPort, int servPort, int nodeNum) {
		this.ip = ip;
		this.pollPort = pollPort;
		this.servPort = servPort;
		this.nodeNum = nodeNum;
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