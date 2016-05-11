import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import twitter4j.HashtagEntity;
import twitter4j.Status;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by trauma on 29/04/16.
 */
public class GraphDb {
    public GraphDatabaseService graphDb;

    public GraphDb() {
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File("data"));
        /*Transaction tx = graphDb.beginTx();
        try {
            graphDb.schema()
                    .constraintFor(DynamicLabel.label("Tweet"))
                    .assertPropertyIsUnique("id")
                    .create();
            graphDb.schema()
                    .constraintFor(DynamicLabel.label("Hashtag"))
                    .assertPropertyIsUnique("text")
                    .create();
            tx.success();
        } finally {
            tx.close();
        }*/
        registerShutdownHook(graphDb);
    }

    public void saveTweet (Status status) {
        Transaction tx = graphDb.beginTx();
        Node tweet = null;
        ResourceIterator<Node> resultIterator = null;
        Map<String, Object> parameters = new HashMap();
        //Query for saving new tweets
        String queryTweet = "MERGE (t:Tweet {id: {id}, author: {author}, timestamp: {timestamp}, " +
                            "text: {text}, isRetweet: {isRetweet}, isAnswer: {isAnswer}})" +
                            "return t";
        try {
            parameters.put("id", status.getId());
            parameters.put("author", status.getUser().getId());
            parameters.put("timestamp", status.getCreatedAt().getTime());
            parameters.put("text", status.getText());
            parameters.put("isRetweet", status.isRetweet());

            //Check if it's an answer: if it is then set the isAnswer field to the previous status
            //Otherwise set it to 0
            if (status.getInReplyToStatusId() != -1)
                parameters.put("isAnswer", status.getInReplyToStatusId());
            else
                parameters.put("isAnswer", 0);
            //Save tweet
            resultIterator = graphDb.execute(queryTweet,parameters).columnAs("t");
            tweet = resultIterator.next();
            saveHashtag(status, tweet);

            tx.success();
        } finally {
            tx.close();
        }
    }
    private void saveHashtag(Status status, Node tweet) {
        //Query for saving hashtags
        String queryHash = "MERGE (t:Hashtag {text: {text}}) return t";
        Map<String, Object> parameters = new HashMap();
        Node tag = null;
        ResourceIterator<Node> resultIterator = null;

        HashtagEntity[] hashtags = status.getHashtagEntities();
        if (hashtags != null){
            for (HashtagEntity hashtag: hashtags) {
                parameters.clear();
                parameters.put("text", hashtag.getText());
                resultIterator = graphDb.execute(queryHash, parameters).columnAs("t");
                tag = null;
                tag = resultIterator.next();
                /*
                //Insert relationship among tweets that have at least a hashtag in common
                hashToTweetRelationships = null;
                hashToTweetRelationships = tag.getRelationships(Direction.OUTGOING, RelType.TAGS);
                if (hashToTweetRelationships != null) {
                    for (Relationship relationship : hashToTweetRelationships) {
                        temp = tweet.createRelationshipTo(relationship.getEndNode(), RelType.SAME_HASHTAG);
                        temp.setProperty("hashtag", tag.getProperty("text"));
                        relationship.delete();
                    }
                }
                */
                //finally insert relationship between current hashtag and current tweet (for each hashtag)
                tag.createRelationshipTo(tweet, RelType.TAGS);
            }
        }
        Relationship currentRel = null;
        for(int i = 0; i < hashtags.length - 1; i++){
            for (int j = i + 1; j < hashtags.length; j++){
                parameters.clear();
                parameters.put("text1", hashtags[i].getText());
                parameters.put("text2", hashtags[j].getText());
                resultIterator = graphDb.execute("MATCH (t1:Hashtag {text: {text1}}) - [r:APPEAR_TOGETHER] - (t2: Hashtag {text: {text2}}) return r", parameters)
                                .columnAs("r");
                if (resultIterator.hasNext()){
                    currentRel = (Relationship) resultIterator.next();
                    currentRel.setProperty("count", (Integer) currentRel.getProperty("count") + 1);
                } else {
                    tag = graphDb.findNode(DynamicLabel.label("Hashtag"),"text", hashtags[i].getText());
                    currentRel = tag.createRelationshipTo(graphDb.findNode(DynamicLabel.label("Hashtag"),"text", hashtags[j].getText()), RelType.APPEAR_TOGETHER);
                    currentRel.setProperty("count", 1);
                }
            }
        }
    }
    private void registerShutdownHook(final GraphDatabaseService graphDb) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                graphDb.shutdown();
            }
        });
    }

}
