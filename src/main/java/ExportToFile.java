import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by trauma on 08/07/16.
 */
public class ExportToFile {

	public static void getFiles (String fileName, Set<String> relatedHashtags, HashMap<Long, String> transactions) {
		printToCsv(fileName, relatedHashtags, transactions);
		//printToText(fileName,transactions);
	}

	private static void printToCsv(String fileName, Set<String> relatedHashtags, HashMap<Long, String> transactions){
		File file = new File("results/" + fileName + ".csv");
		file.getParentFile().mkdirs();
		PrintWriter writer = null;
		try{
			writer = new PrintWriter(file);
			Iterator<String> iterator = relatedHashtags.iterator();
			String init = "";
			while (iterator.hasNext()){
				init += "\'" + iterator.next() + "\',";
			}
			writer.println(init.substring(0,init.length()-1));

			Iterator it = transactions.entrySet().iterator();
			while (it.hasNext()){
				Map.Entry pair = (Map.Entry) it.next();
				writer.println(getPrintableString(relatedHashtags, (String) pair.getValue()));
				it.remove();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			writer.flush();
			writer.close();
		}
	}
	private static void printToText (String fileName, HashMap<Long, String> transactions) {
		File file = new File("results/" + fileName + ".txt");
		file.getParentFile().mkdirs();
		PrintWriter writer = null;
		try {
			writer = new PrintWriter(file);
			Iterator iterator = transactions.entrySet().iterator();
			while(iterator.hasNext()) {
				Map.Entry pair = (Map.Entry) iterator.next();
				writer.println(pair.getValue());
				iterator.remove();
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
		List<String> currentHashtagList = Arrays.asList(currentHashtags.split(","));
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