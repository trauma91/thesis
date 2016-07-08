import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by trauma on 08/07/16.
 */
public class ExportToFile {

	public static void getFiles (String fileName, Set<String> relatedHashtags, ArrayList<String> transactions) {
		printToCsv(fileName, relatedHashtags, transactions);
		printToText(fileName,transactions);
	}

	private static void printToCsv(String fileName, Set<String> relatedHashtags, ArrayList<String> transactions){
		PrintWriter writer = null;
		try{
			writer = new PrintWriter(fileName + ".csv");
			Iterator<String> iterator = relatedHashtags.iterator();
			String init = "";
			while (iterator.hasNext()){
				init += "\'" + iterator.next() + "\',";
			}
			writer.println(init.substring(0,init.length()-1));

			iterator = transactions.iterator();
			while (iterator.hasNext()){
				writer.println(getPrintableString(relatedHashtags, iterator.next()));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			writer.flush();
			writer.close();
		}
	}
	private static void printToText (String fileName, ArrayList<String> transactions) {
		PrintWriter writer = null;
		try {
			writer = new PrintWriter(fileName + ".txt");
			Iterator<String> iterator = transactions.iterator();
			while(iterator.hasNext()) {
				writer.println(iterator.next());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			writer.flush();
			writer.close();
		}
	}
	private static String getPrintableString(Set<String> relatedHashtags, String currentHashtags) {
		String printable = "";
		ArrayList<String> currentHashtagList = (ArrayList<String>) Arrays.asList(currentHashtags.split(","));
		Iterator<String> iterator = relatedHashtags.iterator();
		while (iterator.hasNext()){
			if (currentHashtags.contains(iterator.next())) {
				printable += "t,";
			} else {
				printable += "?,";
			}
		}
		return printable.substring(0,printable.length()-1);
	}
}