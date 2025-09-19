package edu.cugb.faft.topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class SinkBolt extends BaseRichBolt {
    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        int count = input.getIntegerByField("count");
        System.out.printf("[Sink] Word: %-10s | Count: %d%n", word, count);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // sink 不往下游发
    }
}
