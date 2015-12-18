package main.java;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

import java.util.Map;

public class ReporterBolt extends BaseRichBolt{
    OutputCollector collector;
    transient RedisConnection<String, String> redis;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        RedisClient client = new RedisClient("localhost", 6379);
        this.redis = client.connect();
        redis.select(2);
        try {
            redis.flushall();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        redis.set(tuple.getString(0),tuple.getString(1));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
