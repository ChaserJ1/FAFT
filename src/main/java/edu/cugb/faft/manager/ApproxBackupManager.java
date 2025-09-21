package edu.cugb.faft.manager;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

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

    private final Map<String,Double> ratioByOperator  = new ConcurrentHashMap<>();

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

    // 新增：批量更新每个算子的采样率
    public synchronized void updateSamplingRatios(Map<String, Double> ratios) {
        if (ratios == null || ratios.isEmpty()) return;
        ratioByOperator.clear();
        ratioByOperator.putAll(ratios);
        System.out.printf("[FAFT RatioUpdate] %s%n", ratioByOperator);
    }

    /**
     * 尝试进行一次采样式备份
     * @param state 当前算子的状态
     */
    public void tryBackup(Map<String, Integer> state) {
        processedCount++;
        if (random.nextDouble() < currentRatio) {
            backupCount++;
            // 实际备份逻辑（此处仅打印日志，实际可以写入文件或持久化存储）
            System.out.printf("[FAFT Backup] Backup triggered | Ratio=%.2f | Processed=%d | Backups=%d%n",
                    currentRatio, processedCount, backupCount);
            // TODO: 后续可考虑写入磁盘，做持久化逻辑
        }
    }

    // 新增重载：按算子进行采样式备份

    /**
     * 采样式备份2.0
     * @param operatorId
     * @param state
     */
    public void tryBackup(String operatorId, Map<String, Integer> state) {
        processedCount++;
        double r = getRatioFor(operatorId);
        if (random.nextDouble() < r) {
            backupCount++;
            System.out.printf("[FAFT Backup] op=%s ratio=%.2f processed=%d backups=%d%n",
                    operatorId, r, processedCount, backupCount);
            // TODO: 真正的备份落盘/持久化逻辑
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
     * 查询算子专属采样率（没有则回退到全局 currentRatio）
     * @param operatorId
     * @return
     */
    private double getRatioFor(String operatorId) {
        if (operatorId == null) return currentRatio;
        return ratioByOperator.getOrDefault(operatorId, currentRatio);
    }

    /**
     * 打印统计信息（可在 Sink 或定时器中调用）
     */
    public void printStats() {
        System.out.printf("[FAFT Stats] Processed=%d | Backups=%d | Ratio=%.2f%n",
                processedCount, backupCount, currentRatio);
    }
}
