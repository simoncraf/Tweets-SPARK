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

public class TwitterStateless {
    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String input = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter Stateless Exercise");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(20));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // Read the language map file by line
        final JavaRDD<String> languageMapLines = jsc
                .sparkContext()
                .textFile(input);
        // transform it to the expected RDD like in Lab 4
        final JavaPairRDD<String, String> languageMap = LanguageMapUtils
                .buildLanguageMap(languageMapLines);

        // prepare the output
        final JavaPairDStream<String, Integer> 
        languageRankStream = 
        	// IMPLEMENT ME
        		stream
                .transformToPair(x -> x
                .mapToPair(tweet -> new Tuple2 <String, Integer> (tweet.getLang(),1)) //Get the language of the tweet and start the counter
                .join(languageMap)//Comparing with the language code from the map csv
                .mapToPair(lang -> new Tuple2 <String, Integer> (lang._2._2, lang._2._1)) //Connecting the language with the code
                .reduceByKey((a,b) -> a+b))//Wrap it
                .mapToPair(a -> new Tuple2 <Integer, String>(a._2, a._1))
                .transformToPair(s -> s.sortByKey(false)) //To sort in descending order
                .mapToPair(b -> new Tuple2<String, Integer>(b._2, b._1));
        		
        // print first 10 results
        languageRankStream.print(10);

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
