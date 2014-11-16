import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

//class for file entries, stores splits and associated node info
public class FileEntry {
	String fileName;
	int numParts; //partitions of a file 
	int rFactor; //replica factor
	int fileLength; //length of file in bytes
	
	int[][] replicaList;
	
	FileEntry(String fileName, int numParts, int rFactor, int fileLength) {
		this.fileName = fileName;
		this.numParts = numParts;
		this.rFactor = rFactor;
		this.fileLength = fileLength;
		
		replicaList = new int[numParts][rFactor];
	}
	
	String getFileName() {
		return fileName;
	}
	
	int getNumParts() {
		return numParts;
	}
	
	int getRFactor() {
		return rFactor;
	}
	
	int getFileLength() {
		return fileLength;
	}
	
	void editList(int numPart, int replica, int node) {
		replicaList[numPart][replica]=node;
	}
	
	int getEntry(int numPart, int replica) {
		return replicaList[numPart][replica];
	}
	
	void printInfo() {
		System.out.println("FileName: " + fileName);
		System.out.println("NumParts: " + numParts);
		System.out.println("RFactor: " + rFactor);
		System.out.println("FileLength: " + fileLength);
	}
}