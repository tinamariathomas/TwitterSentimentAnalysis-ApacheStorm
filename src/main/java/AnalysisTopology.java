package main.java;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class AnalysisTopology {

    public static void main(String[] args) throws InterruptedException {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("tweets", new TweetSpout(), 10);

        Config conf = new Config();

        conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("sentiment-analysis", conf, topologyBuilder.createTopology());

    }

}

