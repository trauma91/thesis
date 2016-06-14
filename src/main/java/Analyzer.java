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
		Map<String, Object> parameters = new HashMap<String, Object>();
		try {
			String query = "MATCH (h:Hashtag) - [t:TAGS] -> (:Tweet) " +
					"RETURN h, count(t) AS occ " +
					"ORDER BY occ DESC LIMIT {number}";
			parameters.put("number", 10);
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

	private static String getPrintableString(Set<String> relatedHashtags, List<String> currentHashtags) {
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
		System.out.println((popHashs.size() + 1) + " - for all of them");
		Scanner scanner = new Scanner(System.in);
		String index = scanner.next();
		String selectedTag = "";
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
		relatedHashtags = getRelHashtags(selected);
		relatedHashtags.addAll(selected);

		Node tweet = null;
		Node tag = null;
		PrintWriter writer = null;
		String init = " ";
		try {
			tx = graphDb.beginTx();
			if (selectedTag == "")
				writer = new PrintWriter("all.csv");
			else
				writer = new PrintWriter(selectedTag + ".csv");

			if (!relatedHashtags.isEmpty()) {
				//Print the first line of the .csv file (hashtags list)
				Iterator<String> iterator = relatedHashtags.iterator();
				while (iterator.hasNext()){
					init += "\'" + iterator.next() + "\',";
				}
				writer.println(init.substring(0,init.length()-1));
				for (String elem : selected) {
					//Retrieve tweets containing hashtags that appear together with the most popular one.
					String query = "MATCH (:Hashtag {text: {text}}) - [a:APPEAR_TOGETHER] - (h1:Hashtag) " +
							"WITH collect(h1) as hashtags " +
							"MATCH (h2:Hashtag) - [:TAGS] - (t:Tweet) " +
							"WHERE h2 IN hashtags " +
							"RETURN DISTINCT t";

					parameters.put("text", elem);
					tweets = graphDb.execute(query, parameters).columnAs("t");
					parameters.clear();

					List<String> currentHashtags = new ArrayList<String>();
					//For each tweet retrieve its hashtags -> hashtags set become a transaction
					while (tweets.hasNext()) {
						tweet = tweets.next();
						currentHashtags = Arrays.asList(((String) tweet.getProperty("hashtags")).split("\\s*,\\s*"));

						String printable = getPrintableString(relatedHashtags, currentHashtags);
						writer.println(printable);
						hashtags = null;
					}
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
