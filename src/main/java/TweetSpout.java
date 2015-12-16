package main.java;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.LinkedBlockingQueue;

public class TweetSpout extends BaseRichSpout {
    String custkey, custsecret;
    String accesstoken, accesssecret;
    SpoutOutputCollector collector;

    TwitterStream twitterStream;

    LinkedBlockingQueue<Status> queue = null;

    private class TweetListener implements StatusListener {

        @Override
        public void onStatus(Status status) {
            if (status.getLang().equals("en")) {
                queue.offer(status);
            }
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice sdn) {
        }

        @Override
        public void onTrackLimitationNotice(int i) {
        }

        @Override
        public void onScrubGeo(long l, long l1) {
        }

        @Override
        public void onStallWarning(StallWarning warning) {
        }

        @Override
        public void onException(Exception e) {
            e.printStackTrace();
        }
    }

    ;

    public TweetSpout() {
        ConfigurationProvider config = new ConfigurationProvider();
        custkey = config.getCustkey();
        custsecret = config.getCustsecret();
        accesstoken = config.getAccesstoken();
        accesssecret = config.getAccesssecret();
    }

    @Override
    public void open(
            Map map,
            TopologyContext topologyContext,
            SpoutOutputCollector spoutOutputCollector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        collector = spoutOutputCollector;
        ConfigurationBuilder config =
                new ConfigurationBuilder()
                        .setOAuthConsumerKey(custkey)
                        .setOAuthConsumerSecret(custsecret)
                        .setOAuthAccessToken(accesstoken)
                        .setOAuthAccessTokenSecret(accesssecret);

        TwitterStreamFactory fact =
                new TwitterStreamFactory(config.build());

        twitterStream = fact.getInstance();
        twitterStream.addListener(new TweetListener());

        twitterStream.sample();
    }

    @Override
    public void nextTuple() {
        Status tweet = queue.poll();

        if (tweet == null) {
            Utils.sleep(50);
            return;
        }
        try {
            collector.emit(new Values(tweet.getText(), tweet.getPlace().getCountry()));
        } catch (NullPointerException e) {

        }
    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void declareOutputFields(
            OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet", "country"));
    }
}
