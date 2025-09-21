package edu.cugb.faft.manager;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ApproxBackupManager {

    // ===== 单例 =====
    private static volatile ApproxBackupManager INSTANCE;
    public static synchronized ApproxBackupManager init(double initRatio, double rmin, double rmax, double step) {
        if (INSTANCE != null) return INSTANCE;
        INSTANCE = new ApproxBackupManager(initRatio, rmin, rmax, step);
        return INSTANCE;
    }
    public static ApproxBackupManager getInstance() {
        if (INSTANCE == null) throw new IllegalStateException("ApproxBackupManager not initialized");
        return INSTANCE;
    }

    // ===== 配置与状态 =====
    private final Random random = new Random();
    private volatile double currentRatio;  // 旧的全局比例（兼容老接口）
    private final double step;             // 调整步长（用于全局缩放/老接口）
    private final double rmin, rmax;

    // 每算子的“基准 r(v)”表（启动/重算时更新）
    private final ConcurrentHashMap<String, Double> ratioByOperator = new ConcurrentHashMap<>();

    // 叠加在“基准 r”上的全局缩放，用于误差闭环（让所有算子一起收/放）
    private volatile double globalScale = 1.0;

    // 统计：全局/每算子处理计数（TPS 差分用）
    private long processedCount = 0L, backupCount = 0L;
    private final ConcurrentHashMap<String, Long> processedByOp = new ConcurrentHashMap<>();

    // 动态重算循环
    private final AtomicBoolean rebalanceStarted = new AtomicBoolean(false);
    private final ScheduledExecutorService rebalanceExec =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "faft-rebalancer");
                t.setDaemon(true);
                return t;
            });

    private ApproxBackupManager(double initRatio, double rmin, double rmax, double step) {
        this.currentRatio = initRatio;
        this.rmin = rmin;
        this.rmax = rmax;
        this.step = step;
    }

    // ====== API：更新整张 per-op 采样率表 ======
    public synchronized void updateSamplingRatios(Map<String, Double> ratios) {
        if (ratios == null || ratios.isEmpty()) return;
        ratioByOperator.clear();
        ratioByOperator.putAll(ratios);
        System.out.println("[FAFT RatioUpdate] " + ratioByOperator);
    }

    // ====== 兼容：旧的全局版 tryBackup（不带 opId）======
    public void tryBackup(Map<String, Integer> state) {
        processedCount++;
        if (random.nextDouble() < currentRatio) {
            backupCount++;
            System.out.printf("[FAFT Backup][GLOBAL] ratio=%.2f processed=%d backups=%d%n",
                    currentRatio, processedCount, backupCount);
        }
    }

    // ====== 新的按算子 tryBackup（用 opId 查表）======
    public void tryBackup(String operatorId, Map<String, Integer> state) {
        processedCount++;
        if (operatorId != null) processedByOp.merge(operatorId, 1L, Long::sum);

        double r = getRatioFor(operatorId);
        if (random.nextDouble() < r) {
            backupCount++;
            System.out.printf("[FAFT Backup][PER-OP] op=%s ratio=%.2f processed=%d backups=%d%n",
                    operatorId, r, processedCount, backupCount);
            // TODO: 真正的备份逻辑（落盘/远端等）
        }
    }

    // 实际使用的 r：基准表 * 全局缩放（并夹到 [rmin,rmax]）
    private double getRatioFor(String operatorId) {
        double base = ratioByOperator.getOrDefault(operatorId, currentRatio);
        return clamp(base * globalScale);
    }
    private double clamp(double x) { return Math.max(rmin, Math.min(rmax, x)); }

    // ====== 误差闭环：调整 globalScale，而非仅改 currentRatio ======
    public synchronized void adjustByError(double Eobs, double Emax) {
        double lower = 0.5 * Emax;      // 滞回下界
        double s = Math.max(0.01, step); // 用 step 作为缩放步长，最低 1%
        if (Eobs > Emax) {
            // 上调：整体放大
            double maxScale = (rmax / Math.max(rmin, averageBase()));
            globalScale = Math.min(globalScale * (1.0 + s), maxScale);
            System.out.printf("[FAFT Adjust] Error=%.4f > %.4f, globalScale=%.3f%n", Eobs, Emax, globalScale);
        } else if (Eobs < lower) {
            // 下调：整体收缩
            double minScale = rmin / Math.max(rmin, averageBase());
            globalScale = Math.max(globalScale * (1.0 - s), minScale);
            System.out.printf("[FAFT Adjust] Error=%.4f < %.4f, globalScale=%.3f%n", Eobs, lower, globalScale);
        }
    }
    private double averageBase() {
        if (ratioByOperator.isEmpty()) return currentRatio;
        double s = 0; for (double v : ratioByOperator.values()) s += v;
        return s / ratioByOperator.size();
    }

    public void printStats() {
        System.out.printf("[FAFT Stats] Processed=%d | Backups=%d | globalScale=%.2f%n",
                processedCount, backupCount, globalScale);
    }

    // ====== Step4：动态重算 I→r（每 periodMs 刷新一次）======
    public void startDynamicRebalance(Map<String, List<String>> dag,
                                      Set<String> sinks,
                                      double alpha, double beta, double gamma,
                                      double impactDelta, double decayAlpha,
                                      double rmin, double rmax,
                                      long periodMs) {
        if (!rebalanceStarted.compareAndSet(false, true)) return; // 避免重复启动

        final Map<String, Long> lastCount = new ConcurrentHashMap<>();
        rebalanceExec.scheduleAtFixedRate(() -> {
            try {
                // A) TPS by diff（每算子）
                Map<String, Double> tps = new HashMap<>();
                for (String op : dag.keySet()) {
                    long now = processedByOp.getOrDefault(op, 0L);
                    long prev = lastCount.getOrDefault(op, 0L);
                    double rate = Math.max(0, now - prev) / (periodMs / 1000.0);
                    tps.put(op, rate);
                    lastCount.put(op, now);
                }
                double maxTps = tps.values().stream().mapToDouble(x -> x).max().orElse(1.0);

                // B) CPU / Mem（进程级近似）
                double cpu = 0.0;
                try {
                    com.sun.management.OperatingSystemMXBean os =
                            (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
                    cpu = Math.max(0.0, os.getProcessCpuLoad()); // 0~1
                } catch (Throwable ignore) {}
                long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
                double mem = (Runtime.getRuntime().maxMemory() > 0)
                        ? (used / (double) Runtime.getRuntime().maxMemory())
                        : 0.0;

                // C) 组装 OperatorInfo（TPS 归一化到 0~1）
                Map<String, edu.cugb.faft.importance.OperatorInfo> infos = new HashMap<>();
                for (String op : dag.keySet()) {
                    double ntps = maxTps == 0 ? 0 : tps.getOrDefault(op, 0.0) / maxTps;
                    infos.put(op, new edu.cugb.faft.importance.OperatorInfo(op, cpu, mem, ntps));
                }

                // D) 调你的评估器重算 I→r
                edu.cugb.faft.importance.NodeImportanceEvaluator.Result res =
                        edu.cugb.faft.importance.NodeImportanceEvaluator.evaluateAndAssignRatios(
                                dag, sinks, infos, alpha, beta, gamma, impactDelta, decayAlpha, rmin, rmax);

                // E) 刷新 per-op 基准 r 表
                updateSamplingRatios(res.R);
                System.out.println("[FAFT Rebalance] R=" + res.R);
            } catch (Throwable t) {
                t.printStackTrace();
                System.err.println("[FAFT Rebalance] failed: " + t.getMessage());
            }
        }, periodMs, periodMs, TimeUnit.MILLISECONDS);
    }
}
