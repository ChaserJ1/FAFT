package edu.cugb.faft.topology;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class FaftTopologyLauncher {
    public static void main(String[] args) throws Exception {
        // 1. 构建拓扑
        TopologyBuilder builder = new TopologyBuilder();

        // 数据源 Spout
        builder.setSpout("source-spout", new SourceSpout(), 1);

        // 拆分句子 -> 单词
        builder.setBolt("split-bolt", new SplitBolt(), 2)
               .shuffleGrouping("source-spout");

        // 过滤停用词
        builder.setBolt("filter-bolt", new FilterBolt(), 2)
               .shuffleGrouping("split-bolt");

        // 近似容错计数器
        builder.setBolt("faft-count-bolt", new FaftCountBolt(), 2)
               .fieldsGrouping("filter-bolt", new org.apache.storm.tuple.Fields("filteredWord"));

        // 下沉，误差反馈调节
        builder.setBolt("faft-sink-bolt", new FaftSinkBolt(), 1)
               .globalGrouping("faft-count-bolt");

        // 2. 配置
        Config conf = new Config();
        conf.setDebug(false);       // 关闭 debug，避免日志过多
        conf.setNumWorkers(2);      // Worker 数量，集群上用
        conf.setMessageTimeoutSecs(30);

        // 3. 根据运行模式提交
        if (args != null && args.length > 0) {
            // 集群模式
            String topologyName = args[0];
            try {
                StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
            } catch (AlreadyAliveException | InvalidTopologyException e) {
                e.printStackTrace();
            }
        } else {
            // 本地模式，方便调试
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("faft-topology-local", conf, builder.createTopology());
            Thread.sleep(30000);  // 跑 30 秒
            cluster.killTopology("faft-topology-local");
            cluster.shutdown();
        }
    }
}
