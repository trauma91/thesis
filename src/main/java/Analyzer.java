import org.neo4j.cypher.internal.compiler.v2_2.ResultIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
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

	public static void main(String[] args) {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File("data"));
		registerShutdownHook(graphDb);
		String popHash = getMostPopularHashtag();
		Map<String, Object> parameters = new HashMap();
		Node tweet = null;
		Node tag = null;
		PrintWriter writer = null;
		try {
			Transaction tx = graphDb.beginTx();
			writer = new PrintWriter("results.csv");
			String query = "MATCH (:Hashtag {text: {text}}) - [a:APPEAR_TOGETHER] - (h1:Hashtag) " +
					"WITH collect(h1) as hashtags " +
					"MATCH (h2:Hashtag) - [:TAGS] - (t:Tweet) " +
					"WHERE h2 IN hashtags " +
					"RETURN DISTINCT t";
			parameters.put("text", popHash);
			tweets = graphDb.execute(query, parameters).columnAs("t");
			while (tweets.hasNext()) {
				tweet = tweets.next();
				query = "MATCH (t:Tweet {id: {id}}) <- [:TAGS] - (h:Hashtag) return h";
				parameters.clear();
				parameters.put("id", tweet.getProperty("id"));
				hashtags = graphDb.execute(query, parameters).columnAs("h");
				while (hashtags.hasNext()) {
					tag = hashtags.next();
					writer.print(tag.getProperty("text"));
					if (hashtags.hasNext())
						writer.print(",");
				}
				writer.println();
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
