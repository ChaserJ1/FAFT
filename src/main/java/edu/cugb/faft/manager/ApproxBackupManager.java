package edu.cugb.faft.manager;

import java.util.Random;

/**
 * ApproxBackupManager
 *
 * 该类用于管理算子的近似备份逻辑，核心思想是通过采样率（backupRatio）控制状态是否被备份，
 * 并根据误差反馈动态调整采样率，从而在保证用户定义的误差阈值前提下减少备份开销。
 */
public class ApproxBackupManager {
    private final String operatorId;    // 算子 ID，用于标识日志输出
    private double backupRatio;         // 当前采样率（0.0 ~ 1.0）
    private final double minRatio;      // 最小采样率（避免过低）
    private final double maxRatio;      // 最大采样率（避免过高）
    private final double step;          // 每次调整的步长
    private final Random random;        // 随机数生成器，用于采样

    // 统计指标
    private int backupCount;            // 执行过的备份次数
    private int tupleCount;             // 处理过的 tuple 总数

    /**
     * 构造函数（默认参数版本）
     * 初始采样率 0.5，最小 0.1，最大 1.0，步长 0.1
     */
    public ApproxBackupManager(String operatorId) {
        this(operatorId, 0.5, 0.1, 1.0, 0.1);
    }

    /**
     * 构造函数（自定义参数版本）
     * @param operatorId 算子 ID
     * @param initRatio 初始采样率
     * @param minRatio 最小采样率
     * @param maxRatio 最大采样率
     * @param step 调整步长
     */
    public ApproxBackupManager(String operatorId, double initRatio,
                               double minRatio, double maxRatio, double step) {
        this.operatorId = operatorId;
        this.backupRatio = initRatio;
        this.minRatio = minRatio;
        this.maxRatio = maxRatio;
        this.step = step;
        this.random = new Random();
    }

    /**
     * 尝试执行一次采样式备份
     * @param state 算子的状态对象
     */
    public void tryBackup(Object state) {
        tupleCount++;  // 累计处理的 tuple 数量
        if (random.nextDouble() < backupRatio) {  // 根据采样率决定是否备份
            backupCount++;
            System.out.printf("[Backup] Operator=%s | Ratio=%.2f | State=%s%n",
                    operatorId, backupRatio, state.toString());
        }
    }

    /**
     * 增加采样率（通常在误差过高时调用）
     */
    public void increaseBackupRatio() {
        backupRatio = Math.min(maxRatio, backupRatio + step);
        System.out.printf("[BackupManager] Operator=%s | Increased ratio -> %.2f%n",
                operatorId, backupRatio);
    }

    /**
     * 降低采样率（通常在误差过低或资源紧张时调用）
     */
    public void decreaseBackupRatio() {
        backupRatio = Math.max(minRatio, backupRatio - step);
        System.out.printf("[BackupManager] Operator=%s | Decreased ratio -> %.2f%n",
                operatorId, backupRatio);
    }

    /**
     * 根据实时误差动态调整采样率
     * @param estimatedError 当前估计误差
     * @param errorThreshold 用户定义的误差阈值
     */
    public void adjustByError(double estimatedError, double errorThreshold) {
        if (estimatedError > errorThreshold) {
            increaseBackupRatio();
        } else if (estimatedError < errorThreshold / 2) {
            decreaseBackupRatio();
        }
    }

    /**
     * 获取当前采样率
     */
    public double getCurrentRatio() {
        return backupRatio;
    }

    /**
     * 打印当前的统计指标（便于在日志里观测）
     */
    public void printStats() {
        System.out.printf("[BackupStats] Operator=%s | Tuples=%d | Backups=%d | Ratio=%.2f%n",
                operatorId, tupleCount, backupCount, backupRatio);
    }
}
