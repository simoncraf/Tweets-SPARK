package upf.edu;

import java.util.*;
import upf.edu.model.HashTagCount;
import upf.edu.storage.DynamoHashTagRepository;

public class TwitterHashtagsReader {

    public static void main(String[] args) throws Exception {
        String lang = args[0];

        DynamoHashTagRepository w = new DynamoHashTagRepository();
        List<HashTagCount> hashtags = w.readTop10(lang); //Get the top 10 hashtags in the language lang

        //Printing the top10 hashtags of the given language.
        System.out.println("Top 10 Hashtags in: " + lang);
        System.out.println("\n--- Hashtag | Appearances ---\n");
        //Print the list in order
        int index = 0;
        for(HashTagCount h : hashtags){
            index += 1;
            System.out.println(index + " " + h.hashTag+" | "+ h.count);
    }
}}