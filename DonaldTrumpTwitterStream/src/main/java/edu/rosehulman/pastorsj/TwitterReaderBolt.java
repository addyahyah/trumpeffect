package edu.rosehulman.pastorsj;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import twitter4j.Status;


public class TwitterReaderBolt implements IRichBolt {
   private OutputCollector collector;

   @Override
   public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      this.collector = collector;
   }

   @Override
   public void execute(Tuple tuple) {
      Status tweet = (Status) tuple.getValueByField("tweet");
      System.out.println("Tweet about Donald Trump: " + tweet.getText());
      if(tweet.getUser().getId() == 25073877) {
    	  System.out.println("Tweet by Donald Trump: " + tweet.getText());
    	  System.out.println("Confirm name " + tweet.getUser().getName());
    	  System.out.println("This is a tweet by Donald Trump");
    	  this.collector.emit(new Values(tweet.getText()));
      }
   }

   @Override
   public void cleanup() {}

   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("hashtag"));
   }
	
   @Override
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }
	
}