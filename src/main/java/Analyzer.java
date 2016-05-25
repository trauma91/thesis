import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

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
	public static String getMostPopularHashtag() {
		tx = graphDb.beginTx();
		try {
			String query = "MATCH (h:Hashtag) - [t:TAGS] -> (:Tweet) " +
						   "RETURN h, count(t) AS occ " +
						   "ORDER BY occ DESC LIMIT 1";
			tweets = graphDb.execute(query).columnAs("h");
			Node hashtag = tweets.next();
			return (String) hashtag.getProperty("text");
		} finally {
			tx.close();
		}
	}
	/*
	 * Get hashtags that occurs together with the most popular one.
	 */
	public static ArrayList<String> getRelHashtags(String popHash){
		ArrayList<String> relatedHashtags = new ArrayList<String>();
		Map<String, Object> parameters = new HashMap<String, Object>();
		String query = "MATCH (:Hashtag {text: {text}}) - [a:APPEAR_TOGETHER] - (h1:Hashtag) RETURN h1";
		parameters.put("text", popHash);
		tx = graphDb.beginTx();
		try {
			hashtags = graphDb.execute(query,parameters).columnAs("h1");
			while (hashtags.hasNext()) {
				relatedHashtags.add((String) hashtags.next().getProperty("text"));
			}
			return relatedHashtags;
		} finally {
			tx.close();
		}
	}

	private static String getPrintableString(ArrayList<String> relatedHashtags, ArrayList<String> currentHashtags) {
		String printable = "";

		for(int i = 0; i < relatedHashtags.size() - 1; i++) {
			if (currentHashtags.contains(relatedHashtags.get(i))) {
				printable += "t,";
			} else {
				printable += "?,";
			}
		}
		if (currentHashtags.contains(relatedHashtags.get(relatedHashtags.size() - 1)))
			printable += "t";
		else
			printable += "?";
		return printable;
	}

	public static void main(String[] args) {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File("data"));
		registerShutdownHook(graphDb);

		Map<String, Object> parameters = new HashMap<String, Object>();
		String popHash = getMostPopularHashtag();
		ArrayList<String> relatedHashtags = getRelHashtags(popHash);

		Node tweet = null;
		Node tag = null;
		PrintWriter writer = null;
		String init = "";
		try {
			tx = graphDb.beginTx();
			writer = new PrintWriter("results.csv");

			//Retrieve tweets containing hashtags that appear together with the most popular one.
			String query = "MATCH (:Hashtag {text: {text}}) - [a:APPEAR_TOGETHER] - (h1:Hashtag) " +
					"WITH collect(h1) as hashtags " +
					"MATCH (h2:Hashtag) - [:TAGS] - (t:Tweet) " +
					"WHERE h2 IN hashtags " +
					"RETURN DISTINCT t";
			parameters.put("text", popHash);
			tweets = graphDb.execute(query, parameters).columnAs("t");
			ArrayList<String> currentHashtags = new ArrayList<String>();
			int i = 0;
			//Print the first line of the .csv file (hashtags list)
			for (i = 0; i < relatedHashtags.size() - 1; i++){
				init += "\'" + relatedHashtags.get(i) + "\',";
			}
			init += "\'" + relatedHashtags.get(i) + "\'";
			writer.println(init);
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
				String printable = getPrintableString(relatedHashtags,currentHashtags);
				writer.println(printable);
			}
			tx.success();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			writer.flush();
			writer.close();
			tx.close();
		}
	}
}
