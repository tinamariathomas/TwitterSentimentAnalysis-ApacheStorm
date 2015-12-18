package main.java;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class AnalysisTopology {

    public static void main(String[] args) throws InterruptedException {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("tweets", new TweetSpout(), 10);
        topologyBuilder.setBolt("sentiments",new SentimentBolt(),10).shuffleGrouping("tweets");
        topologyBuilder.setBolt("sentimentsPerCountry",new SentimentPercentageBolt(),10).fieldsGrouping("sentiments",new Fields("country"));
        topologyBuilder.setBolt("reporterBolt",new ReporterBolt(),1).globalGrouping("sentimentsPerCountry");

        Config conf = new Config();

        conf.setDebug(true);
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("sentiment-analysis", conf, topologyBuilder.createTopology());

    }

}

