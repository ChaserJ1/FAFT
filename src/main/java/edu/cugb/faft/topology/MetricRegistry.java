package edu.cugb.faft.topology;

import edu.cugb.faft.pojo.OperatorInfo;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.HashMap;

/**
 * 简单的全局指标注册表（本地测试用）
 * Bolt/Spout 定期把指标写入，Monitor 读取快照进行重要性计算。
 */
public class MetricRegistry {
    private static final ConcurrentHashMap<String, OperatorInfo> metrics = new ConcurrentHashMap<>();

    public static void update(String id, double cpuUsage, double memoryUsage) {
        metrics.put(id, new OperatorInfo(id, cpuUsage, memoryUsage));
    }

    public static Map<String, OperatorInfo> snapshot() {
        return new HashMap<>(metrics);
    }
}
