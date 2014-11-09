import java.lang.*;
import java.util.*;
import java.net.*;
import java.io.*;

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