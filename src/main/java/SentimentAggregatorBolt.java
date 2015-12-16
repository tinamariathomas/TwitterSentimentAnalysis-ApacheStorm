package main.java;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SentimentAggregatorBolt extends BaseRichBolt {
    OutputCollector outputCollector;
    Map<String, SentimentMap> countrySentimentMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        countrySentimentMap = new HashMap<String, SentimentMap>();
    }

    @Override
    public void execute(Tuple tuple) {
        String country = tuple.getString(0);
        String sentiment = tuple.getString(1);
        if (countrySentimentMap.containsKey(country)) {
            SentimentMap map = countrySentimentMap.get(country);
            if (map.containsKey(sentiment))
                map.put(sentiment, map.get(sentiment) + 1);
            else
                map.put(sentiment, 1);
        } else {
            SentimentMap map = new SentimentMap();
            map.put(sentiment, 1);
            countrySentimentMap.put(country, map);
        }
        outputCollector.emit(new Values(country,sentiment,countrySentimentMap.get(country).get(sentiment)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
