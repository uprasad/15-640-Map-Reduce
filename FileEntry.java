import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

//class for file entries, stores splits and associated node info
public class FileEntry {
	String fileName;
	int rFactor; //replica factor
	
	int[] replicaList;
	
	FileEntry(String fileName, int rFactor) {
		this.fileName = fileName;
		this.rFactor = rFactor;
		
		replicaList = new int[rFactor];
	}
	
	String getFileName() {
		return fileName;
	}
	
	int getRFactor() {
		return rFactor;
	}
	
	void editList(int replica, int node) {
		replicaList[replica]=node;
	}
	
	int getEntry(int replica) {
		return replicaList[replica];
	}
	
	void printInfo() {
		System.out.println("FileName: " + fileName);
		System.out.println("RFactor: " + rFactor);
	}
}