package edu.cugb.faft.importance;

/**
 * 算子资源/性能占位信息：
 * - cpuUsage / memUsage：0.0 ~ 1.0 归一化值（若无实时指标，可以临时估计或静态配置）
 * - tps：每秒处理的 tuple 数（相对量纲，用于描述繁忙度）
 * 实际生产可用 Storm Metrics 上报来填充这些值。
 */
public class OperatorInfo {
    public final String id;
    public final double cpuUsage;   // 0~1
    public final double memUsage;   // 0~1
    public final double tps;        // 每秒元组数（相对）

    public OperatorInfo(String id, double cpuUsage, double memUsage, double tps) {
        this.id = id;
        this.cpuUsage = cpuUsage;
        this.memUsage = memUsage;
        this.tps = tps;
    }
}
