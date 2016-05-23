import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterListener {
    private static final String CONSUMER_KEY = "gvT4qnVYIh9DRxl5b8SCnkljA";
    private static final String CONSUMER_SECRET = "DdY96PO7FEUHJfr472PxxlgjbxFbahDKC0PKIuRmT7FKwpDplB";
    private static final String ACCESS_TOKEN = "435853131-QUR1h5EcaXUJbP7cjqEhLiQWH3boloHt1BXLhUgs";
    private static final String ACCESS_TOKEN_SECRET = "huoP2IDyWu90HPHT0alcZZNS4Jv6oRbQXWTjzGaI9IOUQ";

    public static ConfigurationBuilder authentication(){
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder
                .setOAuthConsumerKey(CONSUMER_KEY)
                .setOAuthConsumerSecret(CONSUMER_SECRET)
                .setOAuthAccessToken(ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET);
        return configurationBuilder;
    }
    public static void main(String[] args) throws TwitterException {
        String DB_USER = null;
        String DB_PASSWORD = null;
        if (args.length != 2 && args.length != 0)
            System.out.println("Error: specifying username and password for mysql user. Leave empty for the default setting root root");
        else {
            if (args.length == 2) {
                DB_USER = args[0];
                DB_PASSWORD = args[1];
            }
            TwitterStream twitterStream;
            twitterStream = new TwitterStreamFactory(authentication().build()).getInstance();
            final DbConnect connection = new DbConnect(DB_USER, DB_PASSWORD);
            final GraphDb graph = new GraphDb();

            StatusListener listener = new StatusListener() {
                public void onStatus(Status status) {
                    //connection.saveTweet(status);
                    graph.saveTweet(status);
                    System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
                }

                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                    System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
                }

                public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                    System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
                }

                public void onScrubGeo(long userId, long upToStatusId) {
                    System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
                }

                public void onStallWarning(StallWarning warning) {
                    System.out.println("Got stall warning:" + warning);
                }

                public void onException(Exception ex) {
                    ex.printStackTrace();
                }
            };
            twitterStream.addListener(listener);
            twitterStream.sample("en");
        }
    }
}
