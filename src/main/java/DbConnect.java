import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import twitter4j.HashtagEntity;
import twitter4j.Status;

public class DbConnect {
    private Connection connection;
    private PreparedStatement stm;
    private ResultSet rs;
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "root";
    private static final String DB_CONNECTION = "jdbc:mysql://localhost:3306/mydb";
    private static final String DB_DRIVER = "com.mysql.jdbc.Driver";


    public DbConnect(){
        try {
            Class.forName(DB_DRIVER);
            connection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
            System.out.println("Database connection established.");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void saveTweet(Status status){
        String query = "INSERT INTO Tweets(id, text, author, timestamp, isRetweet, isAnswer) VALUES (?,?,?,?,?,?)";
        try {
            //Insert new tweet
            stm = connection.prepareStatement(query);
            stm.setLong(1, status.getId());
            stm.setString(2, status.getText());
            stm.setLong(3, status.getUser().getId());
            stm.setTimestamp(4, new java.sql.Timestamp(status.getCreatedAt().getTime()));
            //Check if it's a retweet
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

            HashtagEntity[] hashtags = status.getHashtagEntities();
            if (hashtags != null) {
                for (HashtagEntity hashtag : hashtags) {
                    //Check if hashtag does not exist and eventually insert it
                    stm.clearParameters();
                    stm = connection.prepareStatement("SELECT * FROM Hashtags WHERE text = ?");
                    stm.setString(1, hashtag.getText());
                    rs = stm.executeQuery();
                    if(!rs.next()) {
                        stm.clearParameters();
                        stm = connection.prepareStatement("INSERT INTO Hashtags(text) VALUES (?)");
                        stm.setString(1, hashtag.getText());
                        stm.executeUpdate();
                    }

                    stm.clearParameters();
                    stm = connection.prepareStatement("SELECT id FROM Hashtags WHERE text = ?");
                    stm.setString(1, hashtag.getText());
                    rs = stm.executeQuery();
                    rs.next();

                    //Insert entry in bridge table
                    stm.clearParameters();
                    stm = connection.prepareStatement("INSERT INTO Tweets_has_Hashtags (tweet_id, hashtag_id) VALUES (?,?)");
                    stm.setLong(1, status.getId());
                    stm.setInt(2, rs.getInt("id"));
                    stm.executeUpdate();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    /*
    public void disconnect(){
        try {
            connection.close();
            stm.close();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }*/
}
