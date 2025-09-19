package edu.cugb.faft.topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

/**
 * 停用词过滤算子
 * 输入字段：word
 * 输出字段：filteredWord
 */

public class FilterBolt extends BaseRichBolt {
    private OutputCollector collector;
    private static final Set<String> STOP = Set.of("the","a","an","in","on","of","and","or");

    @Override public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override public void execute(Tuple input) {
        String word = input.getStringByField("word");
        if (!STOP.contains(word)) {
            collector.emit(input, new Values(word));
        }
        collector.ack(input);
    }

    @Override public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("filteredWord"));
    }
}
