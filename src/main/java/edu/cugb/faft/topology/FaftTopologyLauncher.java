package edu.cugb.faft.topology;

import edu.cugb.faft.importance.NodeImportanceEvaluator;
import edu.cugb.faft.importance.OperatorInfo;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

import java.util.*;

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

        // 2. 配置 计算 O/D/C -> I -> r，并随拓扑下发
        Config conf = new Config();
        conf.setDebug(false);       // 关闭 debug，避免日志过多
        conf.setNumWorkers(2);      // Worker 数量，集群上用
        conf.setMessageTimeoutSecs(30);

        // 发射加速
        conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 1.0);

        // 2.1 DAG：componentId -> children（用 ArrayList 以防某些序列化问题）
        Map<String, List<String>> dag = new HashMap<>();
        dag.put("source-spout",    new ArrayList<>(List.of("split-bolt")));
        dag.put("split-bolt",      new ArrayList<>(List.of("filter-bolt")));
        dag.put("filter-bolt",     new ArrayList<>(List.of("faft-count-bolt")));
        dag.put("faft-count-bolt", new ArrayList<>(List.of("faft-sink-bolt")));
        dag.put("faft-sink-bolt",  new ArrayList<>());

        // sinks
        List<String> sinkList = new ArrayList<>(List.of("faft-sink-bolt"));

        // 放入 conf，供各 bolt 读取并启动动态重算
        conf.put("faft.dag", dag);
        conf.put("faft.sinks", sinkList);

        // --- 2.2 OperatorInfo 占位（0~1 的相对值；之后会换成实时指标） ---
        Map<String, OperatorInfo> infos = new HashMap<>();
        // cpu, mem, tps（都已归一化到 0~1），这里先拉开差距便于观察采样差异
        infos.put("split-bolt",      new OperatorInfo("split-bolt",      0.20, 0.20, 0.30));
        infos.put("filter-bolt",     new OperatorInfo("filter-bolt",     0.30, 0.30, 0.40));
        infos.put("faft-count-bolt", new OperatorInfo("faft-count-bolt", 0.70, 0.60, 0.90)); // 负载更高
        infos.put("faft-sink-bolt",  new OperatorInfo("faft-sink-bolt",  0.10, 0.10, 0.20));

        // --- 2.3 重要性权重与参数（先用默认值；可挪到 yml 配置） ---
        double alpha = 0.34, beta = 0.33, gamma = 0.33; // I(v) = αO + βD + γC
        double impactDelta = 0.9;   // = delta, 供 O(v) 衰减
        double decayAlpha  = 0.9;   // 供 D(v) 衰减
        double rmin = 0.10, rmax = 1.00;

        HashSet<String> sinks = new HashSet<>(sinkList);
        NodeImportanceEvaluator.Result res =
                NodeImportanceEvaluator.evaluateAndAssignRatios(
                        dag, sinks, infos,
                        alpha, beta, gamma,
                        impactDelta,   // delta for O(v)
                        decayAlpha,    // decayAlpha for D(v)
                        rmin, rmax
                );

        // --- 2.4 下发到 Storm Config，供各 Bolt 在 prepare() 读取 ---
        Map<String, Double> ratios = res.R; // 每个算子的采样率
        System.out.println("[FAFT Init] ratios=" + ratios);
        conf.put("faft.ratios", ratios);

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
