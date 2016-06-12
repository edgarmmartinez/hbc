/**
 * Copyright 2013 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.twitter.hbc.example;

import java.util.*;
import java.lang.*;
import java.io.IOException;
import java.text.SimpleDateFormat;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

//      Imports for parsing JSON
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

//      Imports for accessing redis DB
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

//      Imports for implementing the word cloud
import com.kennycason.kumo.WordFrequency;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.bg.CircleBackground;
import com.kennycason.kumo.bg.PixelBoundryBackground;
import com.kennycason.kumo.palette.ColorPalette;
import com.kennycason.kumo.font.scale.SqrtFontScalar;
import java.io.File;
import java.io.FileInputStream;
import java.awt.Color;
import java.awt.Dimension;

//	https://gist.github.com/joelittlejohn/5565410
import com.twitter.hbc.example.TtlHashMap;
import java.util.concurrent.TimeUnit;

import org.isaseb.utils.RankQueue;
import org.isaseb.utils.TimedRankQueue;

public class StreamRankQueue {
    static      List<Map.Entry<String, Integer>> sortHashtable (Hashtable<String,Integer> table) {
        List<Map.Entry<String, Integer>> entries =
                  new ArrayList<Map.Entry<String, Integer>>(table.entrySet());
                Collections.sort(entries, new Comparator<Map.Entry<String, Integer>>() {
                  public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b){
                    return b.getValue().compareTo(a.getValue());
                  }
                });
        
        return entries;
    }
    
    static void printTagList (Hashtable<String, Integer> table, int lowestVal, int maxCount) {
        List<Map.Entry<String, Integer>>        list = sortHashtable (table);
        Iterator        it = list.iterator();
        int     count = 0;
        
        while (it.hasNext() && count < maxCount) {
            Map.Entry entry = (Map.Entry) it.next();
            if (((Integer)entry.getValue()).intValue() >= lowestVal) {
                System.out.println (entry.getKey() + ":" + entry.getValue());
            }
            count += 1;
        }
    }

    static List<Map.Entry<String, Integer>>	sortHashMapByValue (TtlHashMap<String,Integer> hashMap) {
    	List<Map.Entry<String, Integer>> entries = new ArrayList<Map.Entry<String,Integer>> (hashMap.entrySet());
    	Collections.sort(entries, new Comparator<Map.Entry<String, Integer>>() {
    		public int compare(Map.Entry<String, Integer> a, Map.Entry<String, Integer> b) {
    			return b.getValue().compareTo(a.getValue());
    		}
    	});
    		
    	return entries;
    }
    
    static void printRankList (List<Map.Entry<String,Integer>> list, int topN) {
    	for (int i = 0; i < topN && i < list.size(); i++) {
    		System.out.println (list.get(i).getKey() + " : " + list.get(i).getValue());
    	}
    }
    
    static void printMapList (TtlHashMap<String, Integer> map, int lowestVal, int maxCount) {
        List<Map.Entry<String, Integer>>        list = sortHashMapByValue (map);
        Iterator        it = list.iterator();
        int     count = 0;
        
        while (it.hasNext() && count < maxCount) {
            Map.Entry entry = (Map.Entry) it.next();
            if (((Integer)entry.getValue()).intValue() >= lowestVal) {
                System.out.println (entry.getKey() + ":" + entry.getValue());
            }
            count += 1;
        }
    }

    static List<WordFrequency> createWordCloud (List<Map.Entry<String,Integer>> list, int topN) {
        List<WordFrequency>     freqList        = new ArrayList<WordFrequency> (topN);
        
        int     count = 0;
        
        for (Map.Entry<String, Integer> entry : list) {
        	WordFrequency freq = new WordFrequency (entry.getKey() + "-" + Integer.toString((int) entry.getValue()), (int) entry.getValue());
        	freqList.add(freq);
        	count += 1;
        	
        	if (count >= topN) break;
        }
        
        return (List<WordFrequency>) freqList;
    }
    
    static void drawWordCloud (List<Map.Entry<String,Integer>> list, int topN) throws IOException {
  	  List<WordFrequency> wordFrequencies = createWordCloud(list, topN);
//  	  wordFrequencies = createWordCloud(redisDb, "hashtags", topN);
        
        final Dimension dimension = new Dimension(1000, 714);
        final WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
        wordCloud.setPadding(2);
//          wordCloud.setBackground(new CircleBackground(300));
        wordCloud.setBackground(new PixelBoundryBackground(new FileInputStream (new File ("twitter_logo_transparent.png"))));
        wordCloud.setColorPalette(new ColorPalette(new Color(0x4055F1), new Color(0x408DF1), new Color(0x40AAF1), new Color(0x40C5F1), new Color(0x40D3F1), new Color(0xFFFFFF)));
        wordCloud.setFontScalar(new SqrtFontScalar(10, 40));
        wordCloud.build(wordFrequencies);
        wordCloud.writeToFile("output/datarank_wordcloud_circle_sqrt_font_" + new SimpleDateFormat("yyyy_MM_dd_HHmmss").format(Calendar.getInstance().getTime()) + ".png");
    }

  public static void run(String consumerKey, String consumerSecret, String token, String secret,
		  int msgCount, int topN, int trendSec, boolean wordCloud, int debug) throws InterruptedException, IOException {
    // Create an appropriately sized blocking queue
    BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);

    // Define our endpoint: By default, delimited=length is set (we need this for our processor)
    // and stall warnings are on.
    StatusesSampleEndpoint endpoint = new StatusesSampleEndpoint();
    endpoint.stallWarnings(false);

    Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);
    //Authentication auth = new com.twitter.hbc.httpclient.auth.BasicAuth(username, password);

    System.out.println ("StreamRankQueue run");
    // Create a new BasicClient. By default gzip is enabled.
    BasicClient client = new ClientBuilder()
            .name("sampleExampleClient")
            .hosts(Constants.STREAM_HOST)
            .endpoint(endpoint)
            .authentication(auth)
            .processor(new StringDelimitedProcessor(queue))
            .build();

    // Establish a connection
    client.connect();

    ObjectMapper mapper = new ObjectMapper();
    TimedRankQueue<String> hashtagRankQueue = new TimedRankQueue<String>(trendSec);
    TimedRankQueue<String> usernameRankQueue = new TimedRankQueue<String>(10);
    
    // Do whatever needs to be done with messages
    for (int msgRead = 0; msgRead < msgCount; ) {
      if (client.isDone()) {
        System.out.println("Client connection closed unexpectedly: " + client.getExitEvent().getMessage());
        break;
      }

      String msg = queue.poll(5, TimeUnit.SECONDS);
      
      if (msg == null) {
        System.out.println("Did not receive a message in 5 seconds");
      } else {
//        System.out.println(msg + ",");

        JsonNode node = mapper.readTree(msg);
        Iterator<String> fieldNames = node.fieldNames();

//        while (fieldNames.hasNext()) {
//            String fieldName = fieldNames.next();
//            System.out.println(fieldName);
//        }
        JsonNode        jText = node.get("text");
        JsonNode        jLang = node.get("lang");
        JsonNode        jUser = node.findValue("screen_name");
        String[]        strArr = null;
        
        if (jText != null && jLang != null) {
            if (jLang.asText().equals("en")) {
//            if (jText != null) {
                strArr = jText.asText().split("\\s+");
                if (debug >= 1) System.out.println (jText.asText());
                for (int i = 0; i < strArr.length; i++) {
                    if (strArr[i].toCharArray() [0] == '#') {
                        if (debug >= 1) System.out.println (strArr[i]);
                        hashtagRankQueue.offer(strArr[i]);
                    }
                }
                
                if (jUser != null) {
                    if (debug >= 2) System.out.println (jUser.asText());
                    usernameRankQueue.offer(jUser.asText());
                }
                
                msgRead++;
                
                if (msgRead % 20 == 0) {
                    System.out.println ("-------------------------------------------");
                	System.out.println(new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(Calendar.getInstance().getTime()));
                	System.out.println("Number of hashtags in list: " + hashtagRankQueue.size());
                	System.out.println("Number of hashtags ranked: " + hashtagRankQueue.keyCount());
                	
                	if (hashtagRankQueue.size() < 50) {
                    	System.out.println("Hashtags in list: " + hashtagRankQueue.toString());
                	}
                	printRankList(hashtagRankQueue.getRank(), 40);
                }
                
                if (wordCloud && msgRead % 5005 == 0) {
                	long time1 = System.currentTimeMillis();
                	drawWordCloud (hashtagRankQueue.getRank(), topN);
                	System.out.println ((System.currentTimeMillis() - time1)/1000 + " seconds");
                }
            }
        }
      }
    }

    client.stop();

    // Print some stats
    System.out.printf("The client read %d messages!\n", client.getStatsTracker().getNumMessages());
  }
  
  public static void main(String[] args) throws IOException {
    try {
    	StreamRankQueue.run(args[0], args[1], args[2], args[3], Integer.parseInt(args[4]), Integer.parseInt(args[5]), Integer.parseInt(args[6]), Boolean.parseBoolean(args[7]), Integer.parseInt(args[8]));
    } catch (InterruptedException e) {
      System.out.println(e);
    }
  }
}

