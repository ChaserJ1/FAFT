package edu.cugb.faft.topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * 默认算子，供默认容错拓扑使用
 */
public class SimpleBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> counts;

    @Override
    public void prepare(Map<String, Object> topoConf,
                        TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
        this.counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        counts.put(word, counts.getOrDefault(word, 0) + 1);
        System.out.printf("Word: %-10s | Count: %d%n", word, counts.get(word));
        collector.ack(input); // Storm 默认容错
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 这里不需要往下游发送数据
    }
}
