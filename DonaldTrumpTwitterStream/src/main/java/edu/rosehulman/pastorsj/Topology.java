package edu.rosehulman.pastorsj;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class Topology {
	private static String getPropValues(String property) throws IOException {
		String result = "";
		InputStream inputStream = null;
		try {
			Properties prop = new Properties();
			
			File f = new File("twitter4j.properties");
			inputStream = new FileInputStream(f);
			prop.load(inputStream);
			// get the property value and print it out
			result = prop.getProperty(property);
		} catch (Exception e) {
			System.out.println("Exception: " + e);
		} finally {
			inputStream.close();
		}
		return result;
	}
	
	public static void main(String[] args) throws Exception {
		
		String consumerKey = getPropValues("oauth.consumerKey");
		String consumerSecret = getPropValues("oauth.consumerSecret");

		String accessToken = getPropValues("oauth.accessToken");
		String accessTokenSecret = getPropValues("oauth.accessTokenSecret");

		Config config = new Config();
		
		config.setNumWorkers(20);
	    config.setMaxSpoutPending(5000);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("twitter-spout",
				new TwitterSampleSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret));

		builder.setBolt("twitter-reader-bolt", new TwitterReaderBolt()).shuffleGrouping("twitter-spout");

//		final LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("TwitterHashtagStorm", config, builder.createTopology());
//
//		Runtime.getRuntime().addShutdownHook(new Thread() {
//			@Override
//			public void run() {
//				cluster.killTopology("TwitterHashtagStorm");
//				cluster.shutdown();
//			}
//		});
      
      StormSubmitter.submitTopology("TwitterHashtagStorm", config, builder.createTopology());

	}
}