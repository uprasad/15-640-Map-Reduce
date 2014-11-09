import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

public class Job {
	static List<TaskDetails> mapList = null;
	int numMappers;
	int jobId;
	
	Job(int numMappers, int jobId) {
		mapList = new ArrayList<TaskDetails>(numMappers);
		this.numMappers = numMappers;
		this.jobId = jobId;
	}
	
	int getJobId() {
		return this.jobId;
	}
	
	int getNumMappers() {
		return this.numMappers;
	}
}