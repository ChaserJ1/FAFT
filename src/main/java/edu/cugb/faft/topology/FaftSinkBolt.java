package edu.cugb.faft.topology;

import edu.cugb.faft.manager.ApproxBackupManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class FaftSinkBolt extends BaseRichBolt {
    private static final double ERROR_THRESHOLD = 0.05; // 用户定义误差阈值
    private ApproxBackupManager backupManager;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.backupManager = new ApproxBackupManager("sink-bolt");
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        int count = input.getIntegerByField("count");

        System.out.printf("[FAFT Sink] Word: %-10s | Count: %d%n", word, count);

        // 模拟误差估计（这里只是演示，真实情况要计算准确率差值）
        double estimatedError = Math.random() * 0.1;

        // 打印误差与阈值
        System.out.printf("[ErrorCheck] EstimatedError=%.4f | Threshold=%.4f%n", estimatedError, ERROR_THRESHOLD);

        // 根据误差调整采样率
        backupManager.adjustByError(estimatedError, ERROR_THRESHOLD);

        // 每 50 个 tuple 打印一次采样率与状态统计
        if (count % 50 == 0) {
            backupManager.printStats();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // Sink 不往下游发
    }
}
