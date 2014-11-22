import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

/*
 * class for File Entries, stores partitions and associated node info
 */
public class FileEntry implements Serializable {
	String fileName; //fileName for entry
	int numParts; //partitions of a file 
	int rFactor; //replica factor
	int fileLength; //length of file in bytes
	
	//list for maintaining nodeNum for a replica of a partition
	int[][] replicaList;
	
	//constructor for initializing the fields
	FileEntry(String fileName, int numParts, int rFactor, int fileLength) {
		this.fileName = fileName;
		this.numParts = numParts;
		this.rFactor = rFactor;
		this.fileLength = fileLength;
		
		replicaList = new int[numParts][rFactor];
	}
	
	//returns fileName
	String getFileName() {
		return fileName;
	}
	
	//returns no. of partitions for the fileEntry
	int getNumParts() {
		return numParts;
	}
	
	//returns no. of replicas for a file
	int getRFactor() {
		return rFactor;
	}
	
	//returns the length of the file
	int getFileLength() {
		return fileLength;
	}
	
	//assign nodeNum for a partition's replica 
	void editList(int numPart, int replica, int node) {
		replicaList[numPart][replica]=node;
	}
	
	//return the nodeNum for a partition's replica
	int getEntry(int numPart, int replica) {
		return replicaList[numPart][replica];
	}
	
	//print the info for the fileName
	void printInfo() {
		System.out.println("FileName: " + fileName);
		System.out.println("NumParts: " + numParts);
		System.out.println("RFactor: " + rFactor);
		System.out.println("FileLength: " + fileLength);
	}
}