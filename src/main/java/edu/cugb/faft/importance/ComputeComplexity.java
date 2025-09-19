package edu.cugb.faft.importance;

/**
 * 计算复杂度 C(v) 的一个简单线性模型：
 * 可通过 (CPU, 内存, 吞吐) 的加权和来估计算子的计算复杂度；
 * 权重可按业务经验或离线标定赋值。
 */
public class ComputeComplexity {
    public static double compute(double cpuUsage, double memUsage, double tps,
                                 double wCpu, double wMem, double wTps) {
        return wCpu * cpuUsage + wMem * memUsage + wTps * tps;
    }
}
