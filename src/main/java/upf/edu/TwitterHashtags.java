package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaRDD;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.storage.DynamoHashTagRepository;
import upf.edu.util.ConfigUtils;
import java.io.IOException;
import java.io.Serializable;

public class TwitterHashtags {

    public static void main(String[] args) throws InterruptedException, IOException {
        String propertiesFile = args[0];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter Example");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // This is needed by spark to write down temporary data
        jsc.checkpoint("/tmp/checkpoint");

        
        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);
        // <IMPLEMENT ME>
        stream.foreachRDD(new VoidFunction<JavaRDD<Status>>()
        		{ //Executed for each RDD
        			@Override
        			public void call(JavaRDD<Status> RDD) throws Exception{
        				RDD.foreach(new VoidFunction<Status>() //Executed for each element of the RDD
        						{
        							@Override
        							public void call(Status status) throws Exception
        							{
        								DynamoHashTagRepository db = new DynamoHashTagRepository();
        								db.write(status);
        							}
        						});
        			}
        		}); //Sends the Tweet to de Dynamo class we have implemented

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
