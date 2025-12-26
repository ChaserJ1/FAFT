package edu.cugb.faft.topology;

import edu.cugb.faft.monitor.DefaultLatencyMonitor;
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
    private long delayMillis;        // 延迟时长

    public ChaosBolt(double failProbability, double delayProbability, long delayMillis) {
        this.failProbability = failProbability;
        this.delayProbability = delayProbability;
        this.delayMillis = delayMillis;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.random = new Random();
        System.out.printf("[ChaosBolt] Initialized with failProb=%.2f, delayProb=%.2f, delay=%dms%n",
                failProbability, delayProbability, delayMillis);

        FaftLatencyMonitor.init();
        DefaultLatencyMonitor.init();
    }

    @Override
    public void execute(Tuple input) {
        try {
            // 模拟随机延迟
            if (random.nextDouble() < delayProbability) {
                Thread.sleep(delayMillis);
                System.out.println("[ChaosBolt] 触发延迟注入 " + delayMillis + "ms");
            }

            // 模拟随机失败
            if (random.nextDouble() < failProbability) {
                throw new RuntimeException();
            }

            // 正常处理：直接转发
            String word = input.getStringByField("filteredWord");
            collector.emit(input, new Values(word));
            collector.ack(input);

        } catch (Exception e) {
            try {
                FaftLatencyMonitor.recordFailure();
                System.err.println("✅ [ChaosBolt-FAFT] 故障时间已记录");
            } catch (Exception zke) {
                System.err.println("❌ [ChaosBolt-FAFT] 写入 Zookeeper 失败: " + zke.getMessage());
            }

            try {
                DefaultLatencyMonitor.recordFailure();
                System.err.println("✅ [ChaosBolt-Default] 故障时间已记录");
            } catch (Exception zke) {
                zke.printStackTrace(); // 就算这里报错也不影响下面的流程
                System.err.println("❌ [ChaosBolt-Default] 写入 Zookeeper 失败: " + zke.getMessage());
            }
            // 打印堆栈上报
            System.err.println("[ChaosBolt] 触发故障注入");
            e.printStackTrace();

            collector.reportError(e);
            collector.fail(input); // Default 拓扑会走重放；FAFT 会触发近似备份恢复
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("filteredWord"));
    }
}
