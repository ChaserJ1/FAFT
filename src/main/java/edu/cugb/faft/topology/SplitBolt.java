package edu.cugb.faft.topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 拆分算子
 * 解析 TaxiID，并透传 Offset。
 */
public class SplitBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            String line = input.getStringByField("sentence");
            Long offset = input.getLongByField("offset"); // 接收 offset

            // 取中间值  数据格式：timestamp, taxiId, ...
            int firstComma = line.indexOf(',');
            int secondComma = line.indexOf(',', firstComma + 1);

            if (firstComma != -1 && secondComma != -1) {
                String taxiId = line.substring(firstComma + 1, secondComma);
                // 向下游发射 (word, offset)
                collector.emit(input, new Values(taxiId, offset));
            }
        } catch (Exception e) {
            // 忽略脏数据
        }
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "offset"));
    }
}
