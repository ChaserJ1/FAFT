package edu.cugb.faft.topology;

import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TrueCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> trueCounts;
    private Set<Long> processedOffsets; // 去重用

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.trueCounts = new HashMap<>();
        this.processedOffsets = new HashSet<>();
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        Long offset = input.getLongByField("offset");

        // 核心：Offset 去重，防止故障重发污染真值
        if (!processedOffsets.contains(offset)) {
            processedOffsets.add(offset);

            int count = trueCounts.getOrDefault(word, 0) + 1;
            trueCounts.put(word, count);

            // 发射真值流：(word, true_count)
            // streamId 使用默认流即可，下游根据 sourceComponent 区分
            collector.emit(input, new Values(word, count));
        }
        collector.ack(input);

        // 简单清理防止内存泄漏
        if (processedOffsets.size() > 500000) processedOffsets.clear();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "true_count"));
    }
}