package edu.cugb.faft.topology;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * 默认算子，供默认容错拓扑使用
 */
public class SimpleSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private final String[] words = {"stream", "storm", "default", "fault", "tolerance"};
    private final Random random = new Random();

    @Override
    public void open(Map<String, Object> conf,
                     TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        // 模拟实时数据流
        String word = words[random.nextInt(words.length)];
        collector.emit(new Values(word), word); // 以 word 作为 messageId，便于 ack/fail
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {}
    }

    @Override
    public void ack(Object msgId) {
        // 默认机制会调用 ack，表示消息处理成功
    }

    @Override
    public void fail(Object msgId) {
        // 默认机制会调用 fail，可选择重发
        collector.emit(new Values(msgId), msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
