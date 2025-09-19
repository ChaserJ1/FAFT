package edu.cugb.faft.topology;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class SourceSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    // 暂定写死的数据，后续从csv文件中读取数据
    private final String[] sentences = {
            "the quick brown fox jumps over the lazy dog",
            "storm makes stream processing easy",
            "approximate fault tolerance reduces overhead",
            "distributed systems need reliability",
            "adaptive backup improves efficiency"
    };
    private final Random rand = new Random();

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        String sentence = sentences[rand.nextInt(sentences.length)];
        collector.emit(new Values(sentence));
        try {
            Thread.sleep(500); // 模拟间隔
        } catch (InterruptedException ignored) {}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}
