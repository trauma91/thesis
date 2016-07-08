import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by trauma on 23/05/16.
 */
public class Analyzer {
	private static GraphDatabaseService graphDb;
	private static ResourceIterator<Node> tweets = null;
	private static ResourceIterator<Node> hashtags = null;
	private static Transaction tx = null;
	private static Scanner scanner = new Scanner(System.in);

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
		Map<String, Object> parameters = new HashMap<String, Object>();
		System.out.println("How many hashtags?");
		Integer number = Integer.parseInt(scanner.next());
		try {
			String query = "MATCH (h:Hashtag) - [t:TAGS] -> (:Tweet) " +
					"RETURN h, count(t) AS occ " +
					"ORDER BY occ DESC LIMIT {number}";
			parameters.put("number", number);
			tweets = graphDb.execute(query, parameters).columnAs("h");
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
	public static Set<String> getRelToOneHashtag(String popHash) {
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

	public static Set<String> getRelHashtags (ArrayList<String> popHashtags) {
		Set<String> relatedHashtags = new HashSet<String>();
		for (String hashtag: popHashtags) {
			relatedHashtags.addAll(getRelToOneHashtag(hashtag));
		}
		return relatedHashtags;
	}

	private static ArrayList<String> getSelectedHashtag (ArrayList<String> popHashs) {
		String selectedTag = "";
		System.out.println("Please, select one");
		for (int i = 0; i < popHashs.size(); i++){
			System.out.println(i + " - " + popHashs.get(i));
		}
		System.out.println(popHashs.size() + " - for specifying a different one");
		System.out.println((popHashs.size() + 1) + " - for all of them");
		String index = scanner.next();
		ArrayList<String> selected = new ArrayList<String>();

		if (Integer.parseInt(index) == popHashs.size()){
			System.out.println("Write your own hashtag");
			selectedTag = scanner.next();
			selected.add(selectedTag);
		} else if (Integer.parseInt(index) == popHashs.size() + 1) {
			selected.addAll(popHashs);

		} else {
			selectedTag = popHashs.get(Integer.parseInt(index));
			selected.add(selectedTag);
		}
		return selected;
	}

	public static void main(String[] args) {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File("data"));
		registerShutdownHook(graphDb);

		Map<String, Object> parameters = new HashMap<String, Object>();
		ArrayList<String> popHashs = getMostPopularHashtags();
		Set<String> relatedHashtags = new HashSet<String>();

		ArrayList<String> selectedHashtags = getSelectedHashtag(popHashs);
		relatedHashtags = getRelHashtags(selectedHashtags);
		relatedHashtags.addAll(selectedHashtags);

		Node tweet = null;
		Node tag = null;
		String fileName = "";
		try {
			tx = graphDb.beginTx();
			if (selectedHashtags.size() > 1 )
				fileName = "all";
			else
				fileName = selectedHashtags.get(0);

			if (!relatedHashtags.isEmpty()) {
				ArrayList<String> transactions = new ArrayList<String>();
				for (String elem : selectedHashtags) {
					//Retrieve tweets containing hashtags that appear together with the most popular one.
					String query = "MATCH (:Hashtag {text: {text}}) - [a:APPEAR_TOGETHER] - (h1:Hashtag) " +
							"WITH collect(h1) as hashtags " +
							"MATCH (h2:Hashtag) - [:TAGS] - (t:Tweet) " +
							"WHERE h2 IN hashtags " +
							"RETURN DISTINCT t";

					parameters.put("text", elem);
					tweets = graphDb.execute(query, parameters).columnAs("t");
					parameters.clear();

					String currentHashtags;
					//For each tweet retrieve its hashtags -> hashtags set become a transaction
					while (tweets.hasNext()) {
						tweet = tweets.next();
						currentHashtags = (String) tweet.getProperty("hashtags");
						transactions.add(currentHashtags);
						hashtags = null;
					}
				}
				ExportToFile.getFiles(fileName,relatedHashtags,transactions);
				tx.success();
			}
		} finally {
			tx.close();
		}
	}
}
