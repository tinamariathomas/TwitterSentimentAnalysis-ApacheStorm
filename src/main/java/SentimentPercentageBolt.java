package main.java;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class SentimentPercentageBolt extends BaseRichBolt {
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
        SentimentMap map;
        if (countrySentimentMap.containsKey(country)) {
            map = countrySentimentMap.get(country);
            if (map.containsKey(sentiment))
                map.put(sentiment, map.get(sentiment) + 1);
            else
                map.put(sentiment, 1);
        } else {
            map = new SentimentMap();
            map.put(sentiment, 1);
            countrySentimentMap.put(country, map);
        }

        int sum = 0;
        for (Integer count : map.values()) {
            sum += count;
        }

        JSONObject sentimentPercentageJson = new JSONObject();
        for (HashMap.Entry<String, Integer> entry : map.entrySet()) {
            double percentage = (double)entry.getValue()*100/sum;
            sentimentPercentageJson.put(entry.getKey(),String.valueOf(Math.round(percentage))+"%");
        }
        outputCollector.emit(new Values(country,sentimentPercentageJson.toJSONString()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("country", "maxSentiment"));
    }
}
