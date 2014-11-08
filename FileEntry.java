import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

//class for file entries, stores splits and associated node info
public class FileEntry {
	String fileName;
	int numSplit; //no. of splits
	int rFactor; //replica factor
	
	int[][] splitList;
	
	FileEntry(String fileName, int numSplit, int rFactor) {
		this.fileName = fileName;
		this.numSplit = numSplit;
		this.rFactor = rFactor;
		
		splitList = new int[numSplit][rFactor];
	}
	
	String getFileName() {
		return fileName;
	}
	
	int getNumSplit() {
		return numSplit;
	}
	
	int getRFactor() {
		return rFactor;
	}
	
	void editList(int split, int replica, int node) {
		splitList[split][replica]=node;
	}
	
	int getEntry(int split, int replica) {
		return splitList[split][replica];
	}
	
	void printInfo() {
		System.out.println("FileName: " + fileName);
		System.out.println("NumSplit: " + numSplit);
		System.out.println("RFactor: " + rFactor);
	}
}