package edu.cugb.faft.topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * 计数算子（storm默认容错策略使用）
 * 输入字段：filteredWord
 * 输出字段：word, count
 */
public class CountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private final Map<String, Integer> counts = new HashMap<>();

    @Override public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override public void execute(Tuple input) {
        String w = input.getStringByField("word");
        counts.put(w, counts.getOrDefault(w, 0) + 1);
        int c = counts.get(w);
        collector.emit(input, new Values(w, c));
        collector.ack(input);
    }

    @Override public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
