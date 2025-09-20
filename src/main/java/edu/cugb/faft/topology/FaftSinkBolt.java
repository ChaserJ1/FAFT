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
    private OutputCollector collector;   // 用于 ack/fail
    private ApproxBackupManager backupManager;
    private final double ERROR_THRESHOLD = 0.05; // 简化版：误差阈值写死
    private final Random random = new Random();
    private Map<String, Integer> result;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.result = new HashMap<>();
        try{
            this.backupManager = ApproxBackupManager.getInstance(); // 单例共享
        }catch (IllegalStateException e){
            // 如果还没初始化，则用默认参数兜底初始化一次
            this.backupManager = ApproxBackupManager.init(
                    0.5,   // 初始采样率
                    0.1,   // 最小采样率
                    1.0,   // 最大采样率
                    0.05   // 步长
            );
        }


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
        collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }
}
