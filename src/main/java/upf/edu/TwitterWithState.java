package upf.edu;

import java.io.IOException;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;

public class TwitterWithState {
    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String language = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter With State");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        //transform to a stream of <userName, userCurrent>
		final JavaPairDStream<String, Integer> tweetUser = stream
				.filter(tweet -> tweet.getLang().equals(language)) //Filter taking just the language that we want
				.mapToPair(user -> new Tuple2<String,Integer>(user.getUser().getScreenName(), 1)) //Get the user and start the counter
				.reduceByKey((a,b) -> a+b); //Wraping the values by username

        //transform to a stream of <userTotal, userName> and get the first 20
        //From documentation of Spark updateStateByKey
        //https://spark.apache.org/docs/latest/streaming-programming-guide.html
		Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
				(values, state) -> {
					Integer newSum = state.or(0);
					for(int i: values)
					{
						newSum += i;
					}
					return Optional.of(newSum);
				};
		JavaPairDStream<String, Integer> userCounter = tweetUser.updateStateByKey(updateFunction); //We update the counter for the user
        //transform to a sorted stream of <userTotal, userName> which is updated each 20 seconds
		final JavaPairDStream<Integer, String> tweetsCountPerUser = userCounter
				.mapToPair(x -> new Tuple2<Integer, String>(x._2, x._1))  //We create the tuple again of username and updated counter
				.transformToPair(rdd -> rdd.sortByKey(false)); //Descending order

		tweetsCountPerUser.print(20);//prints the 20 users with the highest amount of tweets in each language
        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}