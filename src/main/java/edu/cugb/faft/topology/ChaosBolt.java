package edu.cugb.faft.topology;


import edu.cugb.faft.monitor.FaftLatencyMonitor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Fields;

import java.util.Map;
import java.util.Random;

public class ChaosBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Random random;

    // 故障注入参数
    private double failProbability; // 失败概率
    private double delayProbability; // 延迟概率
    private long delay;        // 延迟时长

    public ChaosBolt(double failProbability, double delayProbability, long delayMillis) {
        this.failProbability = failProbability;
        this.delayProbability = delayProbability;
        this.delay = delayMillis;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.random = new Random();

        System.out.printf("[ChaosBolt] Initialized with failProb=%.2f, delayProb=%.2f, delay=%dms%n",
                failProbability, delayProbability, delay);

        // 1. 获取配置中的 ZK 地址
        String zkStr = (String) topoConf.get("faft.zk.connect");

        if (zkStr == null) {
            zkStr = "127.0.0.1:2181"; // 默认兜底
        }

        // 2. 注入给 Monitor (只是存个字符串，不联网)
        FaftLatencyMonitor.setZkConnect(zkStr);
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        String type = input.getStringByField("type");

        // 🛡️ 1. 保护基准流：REAL 类型直接放行，不做任何干扰
        if ("TYPE_REAL".equals(type)) {
            collector.emit(input, new Values(word, type));
            collector.ack(input);
            return;
        }

        // ⚔️ 2. 攻击实验流：APPROX 类型
        if (random.nextDouble() < failProbability) {
            // === 触发故障 ===
            System.out.println("⚡ [Chaos] 击落实验流数据: " + word + " | 发送崩溃信号...");

            FaftLatencyMonitor.recordFailure(); // 记录时间

            // 发送崩溃信号 (替代原始数据)
            collector.emit(input, new Values("FAFT_CRASH_SIGNAL", "TYPE_APPROX"));

            // 🔥 关键：手动 ACK，告诉 Spout "处理成功"，防止 Spout 重发这条数据
            // 这样真值流拿到了数据，实验流丢了数据，误差就产生了
            collector.ack(input);
        } else {
            // === 正常情况 ===
            // 模拟随机延迟
            if (random.nextDouble() < delayProbability) {
                try { Thread.sleep(delay); } catch (InterruptedException e) {}
            }
            collector.emit(input, new Values(word, type));
            collector.ack(input);
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "type"));
    }
}
