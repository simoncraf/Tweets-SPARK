package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;
import upf.edu.util.LanguageMapUtils;

import java.io.IOException;

public class TwitterWithWindow {
    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String input = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter with windows");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // Read the language map file as RDD
        final JavaRDD<String> languageMapLines = jsc
                .sparkContext()
                .textFile(input);
        final JavaPairRDD<String, String> languageMap = LanguageMapUtils
                .buildLanguageMap(languageMapLines);

        // create an initial stream that counts language within the batch (as in the previous exercise)
        final JavaPairDStream<String, Integer>  // IMPLEMENT ME
        languageCountStream = stream
        .transformToPair(x -> x.mapToPair(tweet -> new Tuple2 <> (tweet.getLang(),1)) //Get the language of the tweet
        .join(languageMap)
        .mapToPair(aux -> new Tuple2 <> (aux._2._2, aux._2._1))); //We use the language map to mach the csv and the text
        
        // Prepare output within the batch
        final JavaPairDStream<Integer, String>  // IMPLEMENT ME
        languageBatchByCount = languageCountStream
        .reduceByKey((a,b) -> a+b) //Wraping the values
        .mapToPair(Tuple2::swap) //Changing the order of the tuple to sort
        .transformToPair(x -> x.sortByKey(false)); //Descending order

        // Prepare output within the window
        final JavaPairDStream<Integer, String>  // IMPLEMENT ME
        languageWindowByCount = languageCountStream
        .window(Durations.seconds(60*5)) //Duration of the window
        .reduceByKey((a,b) -> a+b) //Wraping the values
        .mapToPair(Tuple2::swap) //Changing the order of the tuple to sort
        .transformToPair(x -> x.sortByKey(false)); //Descending order
        ;

        // Print first 15 results for each one
        languageBatchByCount.print(15);
        languageWindowByCount.print(15);

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
