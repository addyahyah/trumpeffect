package edu.rosehulman.pastorsj;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.shade.org.json.simple.JSONArray;
import org.apache.storm.shade.org.json.simple.JSONObject;
import org.apache.storm.shade.org.json.simple.parser.JSONParser;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class Topology {
   public static void main(String[] args) throws Exception{
      Config config = new Config();
      config.setDebug(true);
      
      config.setNumWorkers(20);
      config.setMaxSpoutPending(5000);
      
      String path = args[0];
      File f = new File(path);
      FileReader fr = new FileReader(f);
      BufferedReader br = new BufferedReader(fr);
      JSONParser parser = new JSONParser();
      String line;
      String file = "";
      while((line = br.readLine()) != null) {
    	  file += line;
      }
      List<String> tickers = new ArrayList<String>();
      br.close();
      System.out.println("File contents" + file);
      System.out.println("Parsed contents" + parser.parse(file));
      Object obj = parser.parse(file);
      JSONObject json = (JSONObject) obj;
      for(Object o : json.keySet()) {
    	  JSONArray	arr = (JSONArray) json.get(o);
    	  String ticker = (String) arr.get(0);
    	  tickers.add(ticker);
      }
      
      System.out.println("Array of Tickers: " + Arrays.toString(tickers.toArray()));
      System.out.println("Number of Tickers: " + tickers.toArray().length);
      
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("yahoo-finance-spout", new YahooFinanceSpout(tickers));

      builder.setBolt("price-cutoff-bolt", new PriceTrackingBolt())
         .fieldsGrouping("yahoo-finance-spout", new Fields("company"));
			
//      final LocalCluster cluster = new LocalCluster();
//      cluster.submitTopology("YahooFinanceStorm", config, builder.createTopology());
//      
//      Runtime.getRuntime().addShutdownHook(new Thread() {
//			@Override
//			public void run() {
//				cluster.killTopology("YahooFinanceStorm");
//				cluster.shutdown();
//			}
//		});
      
      StormSubmitter.submitTopology("YahooFinanceStorm", config, builder.createTopology());

   }
}
