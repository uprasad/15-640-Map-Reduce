import java.lang.*;
import java.util.*;

public class SortPb {
	public static void main(String args[]) {
		ProcessBuilder pb = new ProcessBuilder("sort", args[0], "-o", args[0]);
		try {
			pb.start();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
