package edu.cugb.faft.topology;

import edu.cugb.faft.manager.ApproxBackupManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class FaftSinkBolt extends BaseRichBolt {
    private ApproxBackupManager backupManager;
    private final double ERROR_THRESHOLD = 0.05; // 简化版：误差阈值写死
    private final Random random = new Random();
    private Map<String, Integer> result;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
                        OutputCollector collector) {
        this.result = new HashMap<>();
        this.backupManager = ApproxBackupManager.getInstance(); // 单例共享
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        int count = tuple.getIntegerByField("count");
        result.put(word, count);

        // 简单日志输出
        System.out.printf("[FAFT Sink] Word=%s | Count=%d%n", word, count);

        // 模拟误差（随机数）
        double estimatedError = random.nextDouble() * 0.1;

        // 根据误差调整采样率
        backupManager.adjustByError(estimatedError, ERROR_THRESHOLD);

        // 定期打印统计信息
        if (count % 10 == 0) {
            backupManager.printStats();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
