import java.io.*;


public class Reduce {
	public static void main(String args[]) {
		BufferedReader br = null;
		BufferedWriter brout = null;
		File fout = null;
		FileOutputStream fos = null;
		BufferedWriter bw = null;
		
		try {
			fout = new File(args[1]);
			fos = new FileOutputStream(fout);
			bw = new BufferedWriter(new OutputStreamWriter(fos));
		} catch (Exception e) {
			e.printStackTrace();
		}

		
		try {
			br = new BufferedReader(new FileReader(args[0]));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		String line;
		String curWord = null;
		int curCount = 0;
		try {
			while ((line = br.readLine()) != null) {
				String[] splits = line.split("\t");
				String nextWord = splits[0];

				if (curWord == null) {
					curWord = nextWord;
				}

				if (nextWord.equals(curWord)) {
					curCount++;
				} else {
					bw.write(curWord + "\t" + Integer.toString(curCount));
					bw.newLine();
					curCount = 1;
					curWord = nextWord;
				}
			}
			bw.close();
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
