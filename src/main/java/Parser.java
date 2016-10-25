import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.Radix;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by trauma on 21/08/16.
 */
public class Parser {
	private static GraphDatabaseService graphDb;
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

	public static Set<String> getRelToOneHashtag(String popHash) {
		Set<String> relatedHashtags = new HashSet<String>();
		Map<String, Object> parameters = new HashMap<String, Object>();
		String getRelHashtags = "MATCH (:Hashtag {text: {text}}) - [t:TAGS] -> (:Tweet) WITH count(t) as totHash " +
				"MATCH (:Hashtag {text: {text}}) - [a:APPEAR_TOGETHER] - (h1:Hashtag) WHERE (a.count*1000)/totHash > 1 RETURN DISTINCT h1";
		Long totHash;
		tx = graphDb.beginTx();
		try {
			parameters.put("text", popHash);
			hashtags = graphDb.execute(getRelHashtags, parameters).columnAs("h1");
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

	private static boolean containsAll(ArrayList<String> selectedHashtag, ArrayList<String> current) {
		// List doesn't support remove(), use ArrayList instead
		for (String o : current) {
			if (!selectedHashtag.remove(o)) // an element in B is not in A!
				return false;
		}
		return true;          // all elements in B are also in A
	}

	public static void main(String[] args) {
		try {
			graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File("data"));
			registerShutdownHook(graphDb);

			Scanner sc = new Scanner(System.in);
			ArrayList<String> selectedHashtag = new ArrayList<String>();
			Set<String> relatedHashtags = new HashSet<String>();
			System.out.println("Write an hashtag");

			selectedHashtag.add(sc.next());

			relatedHashtags = getRelHashtags(selectedHashtag);
			relatedHashtags.addAll(selectedHashtag);

			Scanner input = new Scanner(new File("rules/all.txt"));
			input.useDelimiter("\n");

			File output = new File("discrepancies/all_" + selectedHashtag + ".txt");
			output.getParentFile().mkdirs();
			PrintWriter writer = new PrintWriter(output);

			String line;
			String regex = "(\\w+)=t|(==>)";
			Pattern pattern = Pattern.compile(regex);
			Matcher m;
			ArrayList<String> current = new ArrayList<String>();
			while (input.hasNext()) {
				line = input.next();
				m = pattern.matcher(line);
				while (m.find()) {
					if (m.group(1) != null) {
						current.add(m.group(1));
					}
				}
				Boolean second = selectedHashtag.containsAll(current);

				if((!Collections.disjoint(current,relatedHashtags)) && (!containsAll(new ArrayList<String>(relatedHashtags), current))) {
					writer.println(line);
					System.out.println(line);
				}
				writer.println();
				current.clear();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
}
