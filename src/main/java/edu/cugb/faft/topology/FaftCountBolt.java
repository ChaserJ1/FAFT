package edu.cugb.faft.topology;

import edu.cugb.faft.manager.ApproxBackupManager;
import edu.cugb.faft.monitor.FaftLatencyMonitor;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class FaftCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private ApproxBackupManager backupManager;
    private String componentId;

    // 双轨状态隔离
    private Map<String, Integer> realCounts;   // 真值 (Ground Truth, 永不丢失)
    private Map<String, Integer> approxCounts; // 实验值 (会崩, 靠备份恢复)

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.componentId = context.getThisComponentId();
        this.realCounts = new HashMap<>();
        this.approxCounts = new HashMap<>();

        // 1. 先使用默认参数初始化 BackupManager，防止配置读取失败导致空指针
        try {
            // 尝试获取单例
            this.backupManager = ApproxBackupManager.getInstance();
        } catch (Exception e) {
            // 如果还没初始化，用默认参数兜底 (0.5采样率)
            this.backupManager = ApproxBackupManager.init(0.5, 0.1, 1.0, 0.05);
        }

        // 2. 尝试从 Storm Config 读取 Launcher 下发的配置
        try {
            // 读取采样率表 (Ratios)
            if (topoConf.containsKey("faft.ratios")) {
                Object ratiosObj = topoConf.get("faft.ratios");
                if (ratiosObj instanceof Map) {
                    Map<String, Double> ratios = (Map<String, Double>) ratiosObj;
                    this.backupManager.updateSamplingRatios(ratios);
                    System.out.println("[FaftCount] 成功加载初始采样率: " + ratios);
                }
            }

            // 读取重要性表 (Importance)
            if (topoConf.containsKey("faft.importance")) {
                Object impObj = topoConf.get("faft.importance");
                if (impObj instanceof Map) {
                    Map<String, Object> rawMap = (Map<String, Object>) impObj;
                    Map<String, Double> impMap = new HashMap<>();

                    for (Map.Entry<String, Object> entry : rawMap.entrySet()) {
                        try {
                            // 强转 Double，兼容 String 类型
                            double val = Double.parseDouble(String.valueOf(entry.getValue()));
                            impMap.put(entry.getKey(), val);
                        } catch (NumberFormatException ignored) {}
                    }

                    this.backupManager.updateImportance(impMap);
                    System.out.println("[FaftCount] 成功加载节点重要性: " + impMap);
                }
            }
        } catch (Exception e) {
            System.err.println("[FaftCount] 读取配置失败，将使用默认参数运行。错误: " + e.getMessage());
            // 不抛出异常，保证程序能继续跑
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            String word = input.getStringByField("word");
            String type = input.getStringByField("type");

            // ============================================
            // 1. 处理基准流 (REAL) - 永远可靠
            // ============================================
            if ("TYPE_REAL".equals(type)) {
                int count = realCounts.getOrDefault(word, 0) + 1;
                realCounts.put(word, count);

                // 发送给 Sink: (word, count, type)
                // 告诉 Sink：这是标准答案
                collector.emit(input, new Values(word, count, "TYPE_REAL"));
                collector.ack(input);
                return;
            }

            // ============================================
            // 2. 处理实验流 (APPROX) - 可能遭遇故障
            // ============================================

            // 2.1 检查崩溃信号
            if ("FAFT_CRASH_SIGNAL".equals(word)) {
                System.out.println("[FaftCount] 收到崩溃信号！模拟内存丢失...");

                // 1. 模拟状态丢失 (只清空近似值，不动真值)
                this.approxCounts.clear();

                // 2. 从近似备份中恢复 (Restore)
                Map<String, Integer> backup = backupManager.getBackup(this.componentId);
                if (backup != null && !backup.isEmpty()) {
                    this.approxCounts.putAll(backup);
                    System.out.println("[FaftCount] 已从近似备份恢复，恢复条目数: " + backup.size());
                }

                // 3. 记录恢复完成时间 (供 Monitor 计算 Latency)
                FaftLatencyMonitor.checkAndRecordRecovery();

                collector.ack(input);
                return; // 信号本身不计入统计
            }

            // 2.2 正常计数 (实验流)
            int count = approxCounts.getOrDefault(word, 0) + 1;
            approxCounts.put(word, count);

            // 2.3 尝试备份 (Backup Strategy)
            // 这里传入 componentId，Manager 会查找对应的采样率决定是否存储
            backupManager.tryBackup(this.componentId, word, count);

            // 发送给 Sink: (word, count, type)
            // 告诉 Sink：这是近似计算的结果
            collector.emit(input, new Values(word, count, "TYPE_APPROX"));
            collector.ack(input);

        } catch (Exception e) {
            e.printStackTrace();
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 输出格式: 单词, 计数值, 类型(REAL/APPROX)
        declarer.declare(new Fields("word", "count", "type"));
    }
}