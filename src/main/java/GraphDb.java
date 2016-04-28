import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import twitter4j.HashtagEntity;
import twitter4j.Status;

/**
 * Created by trauma on 26/04/16.
 */
public class GraphDb {
    private static final String DB_DRIVER = "org.neo4j.jdbc.Driver";
    private static final String DB_CONNECTION = "jdbc:neo4j://localhost:7474";
    private static final String DB_USER = "neo4j";
    private static final String DB_PASSWORD = "root";
    private Connection connection;
    private PreparedStatement stm;

    public GraphDb(){
        try {
            Class.forName(DB_DRIVER);
            connection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
            System.out.println("Graph database connection established.");
            //Create uniqueness constraints on tweet.id and hashtag.text
            stm = connection.prepareStatement("CREATE CONSTRAINT ON (tweet:Tweet) ASSERT tweet.id IS UNIQUE " +
                                              "CREATE CONSTRAINT ON (hashtag:Hashtags) ASSERT hashtag.text IS UNIQUE");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void saveTweet(Status status){
        String queryTweet = "MERGE (tweet:Tweet {id:{1}}) " +
                       "SET tweet.text = {2} " +
                       "SET tweet.author = {3} " +
                       "SET tweet.timestamp = {4} " +
                       "SET tweet.isRetweet = {5} " +
                       "SET tweet.isAnswer = {6}";

        try {
            stm = connection.prepareStatement(queryTweet);
            stm.setLong(1, status.getId());
            stm.setString(2, status.getText());
            stm.setLong(3, status.getUser().getId());
            stm.setLong(4, status.getCreatedAt().getTime());

            if (status.isRetweet())
                stm.setBoolean(5, true);
            else
                stm.setBoolean(5, false);
            //Check if it's an answer: if it is then set the isAnswer field to the previous status
            //Otherwise set it to 0
            if (status.getInReplyToStatusId() != -1)
                stm.setLong(6, status.getInReplyToStatusId());
            else
                stm.setLong(6, 0);
            stm.executeUpdate();
             //Create nodes for hashtags and relationships between hashtags and tweet
            String queryHash = "MATCH (tweet:Tweet {id:{1}}) " +
                    "MERGE (tag:Hashtags {text:{2}}) " +
                    "MERGE (tag)-[:Tags]->(tweet)";
            HashtagEntity[] hashtags = status.getHashtagEntities();
            if (hashtags != null){
                for (HashtagEntity hashtag: hashtags) {
                    stm.clearParameters();
                    stm = connection.prepareStatement(queryHash);
                    stm.setLong(1, status.getId());
                    stm.setString(2, hashtag.getText());
                    stm.executeUpdate();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
