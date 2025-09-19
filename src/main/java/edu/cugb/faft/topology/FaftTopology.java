package edu.cugb.faft.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class FaftTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("source-spout", new SourceSpout(), 2);
        builder.setBolt("split-bolt", new SplitBolt(), 4).shuffleGrouping("source-spout");
        builder.setBolt("filter-bolt", new FilterBolt(), 2).shuffleGrouping("split-bolt");
        builder.setBolt("count-bolt", new FaftCountBolt(), 4).fieldsGrouping("filter-bolt", new Fields("filteredWord"));
        builder.setBolt("sink-bolt", new FaftSinkBolt(), 1).shuffleGrouping("count-bolt");

        Config config = new Config();
        config.setNumWorkers(2);
        config.setDebug(true);

        if (args != null && args.length > 0) {
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("faft-dag-topology", config, builder.createTopology());
            Thread.sleep(60000); // 本地运行 60 秒
            cluster.killTopology("faft-dag-topology");
            cluster.shutdown();
        }
    }
}
