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

@SuppressWarnings("unchecked")
public class FaftSinkBolt extends BaseRichBolt {
    private OutputCollector collector;   // 用于 ack/fail
    private ApproxBackupManager backupManager;
    private final double ERROR_THRESHOLD = 0.05; // 简化版：误差阈值写死
    private final Random random = new Random();
    private Map<String, Integer> result;

    private String operatorId; // 记录该算子的 componentId，供按算子采样


    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.result = new HashMap<>();
        this.operatorId = context.getThisComponentId();
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
        // 从拓扑配置中接收“每算子采样率表”，并下发给管理器
        Object ratios = topoConf.get("faft.ratios");
        if (ratios instanceof Map) {
            this.backupManager.updateSamplingRatios((Map<String, Double>) ratios);
            System.out.println("[FAFT RatioUpdate] sink received ratios=" + ratios);
        }


    }


    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        int count = tuple.getIntegerByField("count");
        result.put(word, count);

        // 简单日志输出
        System.out.printf("[FAFT Sink] Word=%s | Count=%d%n", word, count);

        // 对 Sink 的聚合态做一次“按算子”采样备份
        backupManager.tryBackup(operatorId, new HashMap<>(result));

        // 模拟误差（随机数），占位，之后换真实误差
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
