package edu.cugb.faft.topology;

import edu.cugb.faft.importance.NodeImportanceEvaluator;
import edu.cugb.faft.manager.ApproxBackupManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class FaftCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> counts;
    private ApproxBackupManager backupManager;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<>();
        this.backupManager = new ApproxBackupManager("count-bolt");
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("filteredWord");

        // 更新单词的累计次数
        counts.put(word, counts.getOrDefault(word, 0) + 1);

        // 当前单词的累计次数
        int currentCount = counts.get(word);

        // 全局 tuple 总数（所有单词的累计和）
        int totalTuples = counts.values().stream().mapToInt(Integer::intValue).sum();

        // 发射结果
        collector.emit(new Values(word, counts.get(word)));
        collector.ack(input);

        // 调用近似备份（根据算子权重调整采样率）
        backupManager.tryBackup(new HashMap<>(counts));

        // 打印日志：包含单词累计值、总tuple数和采样率
        System.out.printf("[FAFT Count] Word: %-10s | Count: %d | TotalTuples: %d | CurrentRatio=%.2f%n",
                word, currentCount, totalTuples, backupManager.getCurrentRatio());

        // 每处理 100 个 tuple 打印一次状态统计
        if (counts.values().stream().mapToInt(Integer::intValue).sum() % 100 == 0) {
            backupManager.printStats();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new org.apache.storm.tuple.Fields("word", "count"));
    }
}
