package edu.cugb.faft.importance;

/**
 * @Author: pengjia
 * @Description:   暂未实现
 */
public class ComputeComplexity {
    /**
     * 计算算子的计算复杂度得分。
     * @param cpuUsage CPU 使用率（0.0 - 1.0）
     * @param memoryUsage 内存使用率（0.0 - 1.0）
     * @param cpuWeight CPU 权重
     * @param memoryWeight 内存权重
     * @return 综合复杂度分数
     */
    public static double calculateComplexity(double cpuUsage, double memoryUsage,
                                             double cpuWeight, double memoryWeight) {
        return cpuWeight * cpuUsage + memoryWeight * memoryUsage;
    }

    // 默认权重版本：CPU 0.6，内存 0.4
    // 后续考虑要根据每个算子的自身情况来考虑或者说根据数据的倾斜度来考虑这个计算复杂性
    // 想想方法？？？ 这一步怎么去计算呢
    public static double calculateComplexity(double cpuUsage, double memoryUsage) {
        return calculateComplexity(cpuUsage, memoryUsage, 0.6, 0.4);
    }
}
