package edu.rosehulman.pastorsj;

import java.util.*;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import yahoofinance.Stock;
import yahoofinance.YahooFinance;

import java.math.BigDecimal;

public class YahooFinanceSpout extends BaseRichSpout {
	private SpoutOutputCollector collector;
	private boolean completed = false;
	private TopologyContext context;
	private List<String> tickers;

	public YahooFinanceSpout(List<String> tickers) {
		this.tickers = tickers;
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.context = context;
		this.collector = collector;
	}

	public void nextTuple() {
		try {
			for(String ticker : this.tickers) {
				Stock stock = YahooFinance.get(ticker);
				BigDecimal price = stock.getQuote().getPrice();
				this.collector.emit(new Values(ticker, price.doubleValue()));
				System.out.println("Ticker: " + ticker);
				System.out.println("Price: " + price.doubleValue());
			}
		} catch (Exception e) {
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("company", "price"));
	}

	public boolean isDistributed() {
		return false;
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}