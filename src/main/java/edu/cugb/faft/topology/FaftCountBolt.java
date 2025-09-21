package edu.cugb.faft.topology;

import edu.cugb.faft.manager.ApproxBackupManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class FaftCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> counts;
    private ApproxBackupManager backupManager;

    private String operatorId;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.operatorId = context.getThisComponentId();
        this.counts = new HashMap<>();

        // 初始化 ApproxBackupManager（单例）
        double initRatio = 0.5; // 简化版写死，完整版会来自算子权重
        double min = 0.1, max = 1.0, step = 0.1;
        this.backupManager = ApproxBackupManager.init(initRatio, min, max, step);

        // 接收拓扑下发的每算子采样率表（会在 Launcher 里下发）
        Object ratios = topoConf.get("faft.ratios");

        if (ratios instanceof Map) {
            //noinspection unchecked
            this.backupManager.updateSamplingRatios((Map<String, Double>) ratios);
            System.out.println("[FAFT RatioUpdate] count received ratios=" + ratios);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("filteredWord");
        int count = counts.getOrDefault(word, 0) + 1;
        counts.put(word, count);

        // 发射到下游
        collector.emit(new Values(word, count));

        // 按算子 ID 尝试近似备份
        backupManager.tryBackup(operatorId, new HashMap<>(counts));

        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
