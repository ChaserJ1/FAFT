package edu.cugb.faft.manager;

import java.util.Map;
import java.util.Random;

/**
 * ApproxBackupManager
 * 近似备份管理器，用于管理算子状态的采样式备份
 * - 提供 tryBackup() 方法：基于当前采样率决定是否进行备份
 * - 提供 adjustByError() 方法：根据误差反馈动态调节采样率
 * - 提供统计功能，方便在日志中观察采样率变化和备份触发情况
 */
public class ApproxBackupManager {

    private static ApproxBackupManager instance;   // 单例模式（保证所有 Bolt 共享采样率状态）

    private double currentRatio;   // 当前采样率
    private final double minRatio; // 最小采样率
    private final double maxRatio; // 最大采样率
    private final double step;     // 每次调节的步长
    private final Random random;

    private long processedCount;   // 已处理的 tuple 数量
    private long backupCount;      // 实际执行备份的次数

    /**
     * 构造函数（仅内部使用，外部统一用 getInstance() 获取）
     */
    private ApproxBackupManager(double initRatio, double minRatio, double maxRatio, double step) {
        this.currentRatio = initRatio;
        this.minRatio = minRatio;
        this.maxRatio = maxRatio;
        this.step = step;
        this.random = new Random();
        this.processedCount = 0;
        this.backupCount = 0;
    }

    /**
     * 单例模式初始化（第一次调用时传入配置参数）
     */
    public static synchronized ApproxBackupManager init(double initRatio, double minRatio, double maxRatio, double step) {
        if (instance == null) {
            instance = new ApproxBackupManager(initRatio, minRatio, maxRatio, step);
        }
        return instance;
    }

    /**
     * 获取单例
     */
    public static ApproxBackupManager getInstance() {
        if (instance == null) {
            throw new IllegalStateException("ApproxBackupManager 未初始化，请先调用 init()");
        }
        return instance;
    }

    /**
     * 尝试进行一次采样式备份
     * @param state 当前算子的状态（例如 word-count map）
     */
    public void tryBackup(Map<String, Integer> state) {
        processedCount++;
        if (random.nextDouble() < currentRatio) {
            backupCount++;
            // 实际备份逻辑（此处仅打印日志，实际可以写入文件或持久化存储）
            System.out.printf("[FAFT Backup] Backup triggered | Ratio=%.2f | Processed=%d | Backups=%d%n",
                    currentRatio, processedCount, backupCount);
        }
    }

    /**
     * 根据误差反馈调整采样率
     * @param estimatedError 当前估计的误差
     * @param threshold      用户定义的误差阈值
     */
    public void adjustByError(double estimatedError, double threshold) {
        if (estimatedError > threshold) {
            // 误差超过阈值 → 提升采样率（增加容错能力）
            currentRatio = Math.min(maxRatio, currentRatio + step);
            System.out.printf("[FAFT Adjust] Error=%.4f > %.4f, Increase Ratio to %.2f%n",
                    estimatedError, threshold, currentRatio);
        } else if (estimatedError < threshold * 0.5) {
            // 误差明显低于阈值 → 降低采样率（节省资源）
            currentRatio = Math.max(minRatio, currentRatio - step);
            System.out.printf("[FAFT Adjust] Error=%.4f < %.4f, Decrease Ratio to %.2f%n",
                    estimatedError, threshold * 0.5, currentRatio);
        }
    }

    /**
     * 获取当前采样率
     */
    public double getCurrentRatio() {
        return currentRatio;
    }

    /**
     * 打印统计信息（可在 Sink 或定时器中调用）
     */
    public void printStats() {
        System.out.printf("[FAFT Stats] Processed=%d | Backups=%d | Ratio=%.2f%n",
                processedCount, backupCount, currentRatio);
    }
}
