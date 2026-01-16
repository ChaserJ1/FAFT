package edu.cugb.faft.topology;

import edu.cugb.faft.importance.NodeImportanceEvaluator;
import edu.cugb.faft.importance.OperatorInfo;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.*;

import static org.apache.commons.lang3.math.NumberUtils.toDouble;

public class FaftTopologyLauncher {
    // 获取 application.yml文件里的数据
    @SuppressWarnings("unchecked")
    private static Map<String, Object> loadFaftConfig(){
        Yaml yaml = new Yaml();
        try(InputStream in = FaftTopologyLauncher.class.getClassLoader().getResourceAsStream("application.yml")){
            if (in == null){
                System.err.println("[FAFT-WARN] application.yml 获取失败，将使用默认参数");
                return null;
            }
            Map<String, Object> obj = yaml.load(in);

            return (Map<String, Object>) obj.get("faft");
        } catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }
    public static void main(String[] args) throws Exception {
        // 1. 构建物理拓扑
        // 数据流向： Source -> Split -> filter -> Chaos -> Count -> Sink
        TopologyBuilder builder = new TopologyBuilder();

        // Spout: 读取文件
        builder.setSpout("source-spout", new FileSourceSpout("faft1.txt", true), 1);

        // Split: 分流 (真轨和近似轨)
        builder.setBolt("split-bolt", new SplitBolt(), 2)
               .shuffleGrouping("source-spout");

        // Filter: 简单过滤 (双轨都过)
        builder.setBolt("filter-bolt", new FilterBolt(), 2)
               .shuffleGrouping("split-bolt");

        // Chaos: 故障注入, 只处理实验轨
        builder.setBolt("chaos-bolt", new ChaosBolt(0.01, 0.05, 50), 2)
                .shuffleGrouping("filter-bolt");

        // Count: 双轨计数与恢复
        builder.setBolt("faft-count-bolt", new FaftCountBolt(), 2)
               .fieldsGrouping("chaos-bolt", new org.apache.storm.tuple.Fields("word"));

        // Sink: 计算全局误差与反馈
        builder.setBolt("faft-sink-bolt", new FaftSinkBolt(), 1)
               .globalGrouping("faft-count-bolt");

        // 2. 算法参数、配置加载
        Config conf = new Config();
        conf.setDebug(false);       // 关闭 debug，避免日志过多
        conf.setNumWorkers(2);      // Worker 数量，集群上用
        conf.setMessageTimeoutSecs(30);
        conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 1.0); // 发射加速

        // 默认参数
        double defAlpha = 0.34;
        double defBeta = 0.33;
        double defGamma = 0.33;

        double impactDelta = 0.9;
        double decayAlpha = 0.9;
        double errorThreshold = 0.05;

        double rmin = 0.1;
        double rmax = 0.9;
        double step = 0.05;

        String zkConnect = "127.0.0.1:2181";

        // 差异化权重 Map
        Map<String, List<Double>> rawWeightsMap = new HashMap<>();

        // 加载yml参数
        Map<String, Object> faftConfig = loadFaftConfig();
        if (faftConfig != null) {
            System.out.println("[FAFT-INFO] 成功加载 application.yml 配置：" + faftConfig);
            // 权重参数
            defAlpha = getDouble(faftConfig, "alpha", defAlpha);
            defBeta = getDouble(faftConfig, "beta", defBeta);
            defGamma = getDouble(faftConfig, "gamma", defGamma);

            // 解析算法超参
            impactDelta = getDouble(faftConfig, "impact-delta", impactDelta);
            decayAlpha = getDouble(faftConfig, "decay-alpha", decayAlpha);
            errorThreshold = getDouble(faftConfig, "error-threshold", errorThreshold);

            rmin = getDouble(faftConfig, "rmin", rmin);
            rmax = getDouble(faftConfig, "rmax", rmax);
            step = getDouble(faftConfig, "step", step);

            // 解析 Zookeeper 地址
            zkConnect = (String) faftConfig.getOrDefault("zk-connect", zkConnect);

            // 解析差异化权重 (weights: map<string, list<double>>)
            if (faftConfig.containsKey("weights")) {
                @SuppressWarnings("unchecked")
                Map<String, List<Double>> wCfg = (Map<String, List<Double>>) faftConfig.get("weights");
                if (wCfg != null) {
                    rawWeightsMap.putAll(wCfg);
                }
            }
        }

        // 放入 config
        conf.put("faft.alpha", defAlpha);
        conf.put("faft.beta",  defBeta);
        conf.put("faft.gamma", defGamma);

        conf.put("faft.impact.delta", impactDelta);
        conf.put("faft.decay.alpha",  decayAlpha);
        conf.put("faft.error.threshold", errorThreshold);

        conf.put("faft.rmin", rmin);
        conf.put("faft.rmax", rmax);
        conf.put("faft.step", step);

        conf.put("faft.zk.connect", zkConnect); // 注入 ZK 地址
        conf.put("faft.weights", rawWeightsMap); // 注入差异化权重表 (原始 Map)

        // 3. 构建逻辑拓扑 DAG & 静态 OperatorInfo
        Map<String, List<String>> dag = new HashMap<>();
        dag.put("source-spout",    new ArrayList<>(List.of("split-bolt")));
        dag.put("split-bolt",      new ArrayList<>(List.of("filter-bolt")));
        dag.put("filter-bolt",     new ArrayList<>(List.of("chaos-bolt")));
        dag.put("chaos-bolt",      new ArrayList<>(List.of("faft-count-bolt")));
        dag.put("faft-count-bolt", new ArrayList<>(List.of("faft-sink-bolt")));
        dag.put("faft-sink-bolt",  new ArrayList<>());

        // sinks
        List<String> sinkList = new ArrayList<>(List.of("faft-sink-bolt"));

        // 放入 conf，供各 bolt 读取并启动动态重算
        conf.put("faft.dag", dag);
        conf.put("faft.sinks", sinkList);

        // 静态 OperatorInfo 占位（0~1 的相对值；之后会换成实时指标）
        Map<String, OperatorInfo> infos = new HashMap<>();
        // cpu, mem, tps（都已归一化到 0~1），这里先拉开差距便于观察采样差异
        infos.put("split-bolt",      new OperatorInfo("split-bolt",      0.20, 0.20, 0.30));
        infos.put("filter-bolt",     new OperatorInfo("filter-bolt",     0.30, 0.30, 0.40));
        infos.put("faft-count-bolt", new OperatorInfo("faft-count-bolt", 0.70, 0.60, 0.90)); // 负载更高
        infos.put("faft-sink-bolt",  new OperatorInfo("faft-sink-bolt",  0.10, 0.10, 0.20));

        // 4. 执行初始重要性评估算法
        // 4.1 准备权重对象
        NodeImportanceEvaluator.Weights defaultWeightsObj =
                new NodeImportanceEvaluator.Weights(defAlpha, defBeta, defGamma);

        Map<String, NodeImportanceEvaluator.Weights> weightsObjMap = new HashMap<>();
        for (Map.Entry<String, List<Double>> entry : rawWeightsMap.entrySet()) {
            List<Double> val = entry.getValue();
            if (val != null && val.size() >= 3) {
                weightsObjMap.put(entry.getKey(),
                        new NodeImportanceEvaluator.Weights(val.get(0), val.get(1), val.get(2)));
            }
        }
        // 4.2 调用评估算法
        HashSet<String> sinks = new HashSet<>(sinkList);
        NodeImportanceEvaluator.Result res =
                NodeImportanceEvaluator.evaluateAndAssignRatios(
                        dag, sinks, infos,
                        weightsObjMap, defaultWeightsObj, // 传入差异化权重
                        impactDelta, decayAlpha,
                        rmin, rmax
                );

        // 4.3 结果下发到 Storm Config，供各 Bolt 在 prepare() 读取
        Map<String, Double> ratios = res.R; // 每个算子的采样率
        System.out.println("[FAFT Init] 初始采样率 ratios = " + ratios);
        conf.put("faft.ratios", ratios);    // 初始采样率

        // importance 转换成 Map<String, String>，保证 JSON 序列化安全
        Map<String, String> importanceStr = new HashMap<>();
        for (Map.Entry<String, Double> e : res.I.entrySet()) {
            importanceStr.put(e.getKey(), String.valueOf(e.getValue()));
        }
        conf.put("faft.importance", importanceStr); // 初始重要性

        System.out.println("[FAFT Init] 初始重要性 importance =" + importanceStr);




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
            System.out.println("Wait for local cluster...");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("faft-topology-local", conf, builder.createTopology());
            System.out.println("✅ 实验已启动！正在运行中...");
            System.out.println("👉 程序正处于死循环保活状态。如需结束，请手动点击 IDEA 红色停止按钮。");
            while (true) {
                try {
                    Thread.sleep(10000); // 每10秒醒一次，不占CPU
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    break;
                }
            }
        }
    }

    private static double getDouble(Map<String, Object> map, String key, double defaultValue) {
        Object val = map.get(key);
        // 如果 val 为 null，String.valueOf 会返回 "null"，toDouble 会解析失败并返回 defaultValue
        return toDouble(String.valueOf(val), defaultValue);
    }
}
