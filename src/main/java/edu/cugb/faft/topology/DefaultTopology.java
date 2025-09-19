package edu.cugb.faft.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class DefaultTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("source-spout", new SourceSpout(), 2);
        builder.setBolt("split-bolt", new SplitBolt(), 4).shuffleGrouping("source-spout");
        builder.setBolt("filter-bolt", new FilterBolt(), 2).shuffleGrouping("split-bolt");
        builder.setBolt("count-bolt", new CountBolt(), 4).fieldsGrouping("filter-bolt", new Fields("word"));
        builder.setBolt("sink-bolt", new SinkBolt(), 1).shuffleGrouping("count-bolt");

        Config config = new Config();
        config.setDebug(true);

        // 提交到集群
        StormSubmitter.submitTopology(args[0], config, builder.createTopology());

    }
}
