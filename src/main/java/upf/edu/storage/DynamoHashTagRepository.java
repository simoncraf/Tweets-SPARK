package upf.edu.storage;

import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import twitter4j.HashtagEntity;
import twitter4j.Status;
//import upf.edu.model.HashTagCount;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;

//import upf.edu.model.HashTagComparator;
import upf.edu.model.HashTagCount;
import upf.edu.storage.IHashtagRepository;


public class DynamoHashTagRepository implements IHashtagRepository, Serializable {
	
	final static String endpoint = "dynamodb.us-east-1.amazonaws.com";
    final static String region = "us-east-1";
    final AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withEndpointConfiguration(
                    new AwsClientBuilder.EndpointConfiguration(endpoint, region)
            ).withCredentials(new ProfileCredentialsProvider("default"))
            .build();
    final DynamoDB dynamoDB = new DynamoDB(client);
    final Table dynamoDBTable = dynamoDB.getTable("LSDS2020-TwitterHashtags");

  @Override
  public void write(Status tweet) {
    // IMPLEMENT ME
	HashtagEntity[] hashtags = tweet.getHashtagEntities();
	if(hashtags != null) { //check if the tweet has hashtags
		for(HashtagEntity hashtag : hashtags) { //for every hashtag we apply the following operations
			String text = hashtag.getText(); //we get the text of the hashtag
			String lang = tweet.getLang(); //we get the language of the tweet
			Long tweet_id = tweet.getId(); //we get the id of the tweet
			
			/*First we check if the hashtag is already in our DB, if it is we will update it
			 * If is not we will add it. We followed AWS documentation.
			 * */
				UpdateItemSpec updateItemSpec = new UpdateItemSpec()
						.withPrimaryKey("hashtag", text)
						.withUpdateExpression("set appearances = appearances +:c, tweets_id=list_append(:l,tweets_id)")
		                .withValueMap(new ValueMap().withNumber(":c",1).withList(":l", tweet_id))
		                .withReturnValues(ReturnValue.UPDATED_NEW);

			    System.out.println(text + " " + lang + " " + tweet_id);

				try { //We try to update it here
					System.out.println("Trying to update the item if it exists...");
		            UpdateItemOutcome outcome = dynamoDBTable.updateItem(updateItemSpec);
		            System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
				} 
				catch (Exception e) { //If the hashtag is not in the DB we add it with the counter set to 1
					System.err.println("Unable to update the item: " + text + " " + lang+ "\n");
					System.err.println(e.getMessage()+ "\n");
					try{
						System.out.println("Adding a new item...");
						PutItemOutcome outcome = dynamoDBTable
								.putItem(new Item().withPrimaryKey("hashtag", text)
										.withString("language",lang)
										.withNumber("appearances", 1)
										.withList("tweets_id", Arrays.asList(tweet_id)));

						System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
					}

					catch (Exception e2){
						System.err.println("Unable to add item: " + text + " " + lang + "\n");
						System.err.println(e2.getMessage() + "\n");
					}
		        }
			
		}
	}
  }

  @Override
  public List<HashTagCount> readTop10(String lang) {
    // IMPLEMENT ME
	List<HashTagCount> topHashtags =  new ArrayList<>();
	ScanSpec scanSpec = new ScanSpec()
			.withFilterExpression("#la = :l") //We search by language
			.withNameMap(new NameMap().with("#la","language")) //language is a protected word
			.withValueMap(new ValueMap().withString(":l",lang)) //Specify the language we want
			;
	try {
		System.out.println("Trying to read values..."); //We try to read
        ItemCollection<ScanOutcome> items = dynamoDBTable.scan(scanSpec);
		Iterator<Item> iter = items.iterator();
		while (iter.hasNext()){ //for every element
			Item item = iter.next();
			//Creating the HashTagCount instance
			HashTagCount h = new HashTagCount(item.getString("hashtag"),lang,item.getLong("appearances"));
			topHashtags.add(h); //Add it to the list of tophashtags
		}

    } catch (Exception e) {
        System.err.println("Unable to read item: " + lang);
        System.err.println(e.getMessage());
    }
	  topHashtags.sort(Comparator.comparing(HashTagCount::getCount).reversed()); //Sorting the array by appearances
	  topHashtags = topHashtags.stream().limit(10).collect(Collectors.toList()); //Taking just the first 10
	  return topHashtags;

  }

}
