import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

/*
 * An implementation of the comparator interface
 * To use to compare two TaskTrackerInfo objects in a PriorityQueue
 */
public class TaskTrackerInfoComparator implements Comparator<TaskTrackerInfo> {
	@Override
	public int compare(TaskTrackerInfo x, TaskTrackerInfo y) {
		if (x.getLoad() < y.getLoad()) {
			return -1;
		} else if (x.getLoad() > y.getLoad()) {
			return 1;
		}
		
		return 0;
	}
}