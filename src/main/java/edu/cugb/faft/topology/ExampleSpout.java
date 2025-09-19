package edu.cugb.faft.topology;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;

import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 简单随机单词生成 Spout，周期上报自身 CPU/内存指标到 MetricRegistry
 */
public class ExampleSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private transient OperatingSystemMXBean osBean;
    private transient ScheduledExecutorService scheduler;
    private final String id;

    private final Random rand = new Random();

    public ExampleSpout(String id) {
        this.id = id;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);

        scheduler = Executors.newSingleThreadScheduledExecutor();
        // 每 5 秒更新一次自身度量
        scheduler.scheduleAtFixedRate(() -> {
            double cpu = osBean.getProcessCpuLoad();
            if (cpu < 0) cpu = 0.0;
            long totalMem = Runtime.getRuntime().totalMemory();
            long freeMem = Runtime.getRuntime().freeMemory();
            double memUsage = totalMem > 0 ? (double)(totalMem - freeMem)/ (double) totalMem : 0.0;
            MetricRegistry.update(id, cpu, memUsage);
        }, 2, 5, TimeUnit.SECONDS);
    }

    @Override
    public void nextTuple() {
        String[] words = new String[]{"storm","faft","test","apache","stream"};
        String w = words[rand.nextInt(words.length)];
        collector.emit(new Values(w));
        Utils.sleep(100); // 控制发射速率
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void close() {
        if (scheduler != null) scheduler.shutdownNow();
    }
}
