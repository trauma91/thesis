import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

/**
 * Created by trauma on 23/05/16.
 */
public class Analyzer {
	private static GraphDatabaseService graphDb;
	private static ResourceIterator<Node> tweets = null;
	private static ResourceIterator<Node> hashtags = null;
	private static Transaction tx = null;

	private static void registerShutdownHook(final GraphDatabaseService graphDb) {
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				graphDb.shutdown();
			}
		});
	}

	/*
	 * Get most popular hashtag, max num of occurrences.
	 */
	public static ArrayList<String> getMostPopularHashtags() {
		tx = graphDb.beginTx();
		ArrayList<String> popHashtags = new ArrayList<String>();
		try {
			String query = "MATCH (h:Hashtag) - [t:TAGS] -> (:Tweet) " +
					"RETURN h, count(t) AS occ " +
					"ORDER BY occ DESC LIMIT 10";
			tweets = graphDb.execute(query).columnAs("h");
			Node hashtag = null;
			while (tweets.hasNext()) {
				popHashtags.add((String) tweets.next().getProperty("text"));
			}
			return popHashtags;
		} finally {
			tx.close();
		}
	}

	/*
	 * Get hashtags that occurs together with the most popular one.
	 */
	public static Set<String> getRelHashtags(String popHash) {
		Set<String> relatedHashtags = new HashSet<String>();
		Map<String, Object> parameters = new HashMap<String, Object>();
		String query = "MATCH (:Hashtag {text: {text}}) - [a:APPEAR_TOGETHER] - (h1:Hashtag) RETURN DISTINCT h1";
		tx = graphDb.beginTx();
		try {
			parameters.put("text", popHash);
			hashtags = graphDb.execute(query, parameters).columnAs("h1");
			while (hashtags.hasNext()) {
				relatedHashtags.add((String) hashtags.next().getProperty("text"));
			}
			return relatedHashtags;
		} finally {
			tx.close();
		}
	}

	private static String getPrintableString(Set<String> relatedHashtags, ArrayList<String> currentHashtags) {
		String printable = "";
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

	public static void main(String[] args) {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File("data"));
		registerShutdownHook(graphDb);

		Map<String, Object> parameters = new HashMap<String, Object>();
		ArrayList<String> popHashs = getMostPopularHashtags();
		Set<String> relatedHashtags = new HashSet<String>();
		System.out.println("Please, select one");
		for (int i = 0; i < popHashs.size(); i++){
			System.out.println(i + " - " + popHashs.get(i));
		}
		System.out.println(popHashs.size() + " - for specifying a different one");
		Scanner scanner = new Scanner(System.in);
		String index = scanner.next();
		String selectedTag;
		if (Integer.parseInt(index) == popHashs.size()){
			System.out.println("Write your own hashtag");
			selectedTag = scanner.next();
			relatedHashtags = getRelHashtags(selectedTag);
		} else {
			selectedTag = popHashs.get(Integer.parseInt(index));
			relatedHashtags = getRelHashtags(selectedTag);
		}
		Node tweet = null;
		Node tag = null;
		PrintWriter writer = null;
		String init = " ";
		try {
			tx = graphDb.beginTx();
			writer = new PrintWriter("results.csv");
			if (!relatedHashtags.isEmpty()) {
				//Print the first line of the .csv file (hashtags list)
				Iterator<String> iterator = relatedHashtags.iterator();
				while (iterator.hasNext()){
					init += "\'" + iterator.next() + "\',";
				}
				writer.println(init.substring(0,init.length()-1));
				//Retrieve tweets containing hashtags that appear together with the most popular one.
				String query = "MATCH (:Hashtag {text: {text}}) - [a:APPEAR_TOGETHER] - (h1:Hashtag) " +
						"WITH collect(h1) as hashtags " +
						"MATCH (h2:Hashtag) - [:TAGS] - (t:Tweet) " +
						"WHERE h2 IN hashtags " +
						"RETURN DISTINCT t";

				parameters.put("text", selectedTag);
				tweets = graphDb.execute(query, parameters).columnAs("t");
				parameters.clear();

				ArrayList<String> currentHashtags = new ArrayList<String>();
				//For each tweet retrieve its hashtags -> hashtags set become a transaction
				while (tweets.hasNext()) {
					tweet = tweets.next();
					query = "MATCH (t:Tweet {id: {id}}) <- [:TAGS] - (h:Hashtag) return h";
					parameters.clear();
					parameters.put("id", tweet.getProperty("id"));
					hashtags = graphDb.execute(query, parameters).columnAs("h");
					while (hashtags.hasNext()) {
						currentHashtags.add((String) hashtags.next().getProperty("text"));
					}
					String printable = getPrintableString(relatedHashtags, currentHashtags);
					writer.println(printable);
					currentHashtags.clear();
				}
				tx.success();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			writer.flush();
			writer.close();
			tx.close();
		}
	}
}
