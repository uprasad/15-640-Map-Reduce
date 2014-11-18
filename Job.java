import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

public class Job implements Serializable{
	static List<TaskDetails> mapList = null;
	static List<TaskDetails> reduceList = null;
	
	int numMappers;
	int numReducers;
	
	int jobId;
	String inputDir; //inputDir name as in the FileSystem
	String outputDir;
	
	Job(int numMappers, int numReducers, int jobId, String inputDir, String outputDir) {
		mapList = new ArrayList<TaskDetails>(numMappers);
		reduceList = new ArrayList<TaskDetails>(numReducers);
		
		this.numMappers = numMappers;
		this.numReducers = numReducers;
		
		this.jobId = jobId;
		this.inputDir = inputDir;
		this.outputDir = outputDir;
	}
	
	int getJobId() {
		return this.jobId;
	}
	
	int getNumMappers() {
		return this.numMappers;
	}
	
	int getNumReducers() {
		return this.numReducers;
	}
	
	String getInputDir() {
		return inputDir;
	}
	
	String getOutputDir() {
		return outputDir;
	}
}