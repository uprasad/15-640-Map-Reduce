import java.io.*;


public class Map {
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
		try {
			while ((line = br.readLine()) != null) {
				String[] splits = line.split(" ");
				for (int i=0; i<splits.length; i++) {
					bw.write(splits[i] + "\t" + Integer.toString(1));
					bw.newLine();
				}
			}
			bw.close();
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
