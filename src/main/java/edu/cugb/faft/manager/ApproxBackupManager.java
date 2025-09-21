package edu.cugb.faft.manager;

import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 近似备份管理器（支持“按算子采样率 + 局部调节”）
 * - ratioByOperator：每算子基准采样率（来自启动时评估/动态重算）
 * - importanceMap  ：每算子重要性分数（来自评估器）
 * - adjustByError  ：当误差超阈值时，仅对“重要算子 Top P%”做上调；当误差远低于阈值时，对“低重要性算子 Bottom P%”做温和下调
 */
public class ApproxBackupManager {

    //单例
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

    // 配置与状态
    private final Random random = new Random();
    private volatile double currentRatio;  // 旧的全局比例（兼容老接口）
    private final double step;             // 调整步长（用于全局缩放/老接口）
    private final double rmin, rmax;

    // 每算子的“基准 r(v)”表（启动/重算时更新）
    private final ConcurrentHashMap<String, Double> ratioByOperator = new ConcurrentHashMap<>();

    // 每算子的“重要性分数”
    private final ConcurrentHashMap<String, Double> importanceMap = new ConcurrentHashMap<>();

    // 叠加在“基准 r”上的全局缩放，用于误差闭环（让所有算子一起收/放），已改为局部调节
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

    // 更新整张 per-op 采样率表
    public synchronized void updateSamplingRatios(Map<String, Double> ratios) {
        if (ratios == null || ratios.isEmpty()) return;
        ratioByOperator.clear();
        ratioByOperator.putAll(ratios);
        System.out.println("[FAFT RatioUpdate] " + ratioByOperator);
    }

    public synchronized void updateImportance(Map<String, Double> importance) {
        if (importance == null || importance.isEmpty()) return;
        importanceMap.clear();
        importanceMap.putAll(importance);
        System.out.println("[FAFT ImportanceUpdate] " + importanceMap);
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

    // 局部调节
    public synchronized void adjustByError(double Eobs, double Emax) {
        double lower = 0.5 * Emax;      // 滞回下界
        double s = Math.max(0.01, step); // 用 step 作为缩放步长，最低 1%
        if (Eobs > Emax) {
            // 上调：只调 Top 30% 重要算子
            List<String> topOps = topKByImportance(true, 0.3);
            for (String op : topOps) {
                double oldR = ratioByOperator.getOrDefault(op, currentRatio);
                double newR = clamp(oldR + step);
                ratioByOperator.put(op, newR);
                System.out.printf("[FAFT Adjust][LOCAL-UP] op=%s oldR=%.3f newR=%.3f (error=%.4f > %.4f)%n",
                        op, oldR, newR, Eobs, Emax);
            }
        } else if (Eobs < lower) {
            // 下调：只调 Bottom 30% 重要算子
            List<String> lowOps = topKByImportance(false, 0.3);
            for (String op : lowOps) {
                double oldR = ratioByOperator.getOrDefault(op, currentRatio);
                double newR = clamp(oldR - step);
                ratioByOperator.put(op, newR);
                System.out.printf("[FAFT Adjust][LOCAL-DOWN] op=%s oldR=%.3f newR=%.3f (error=%.4f < %.4f)%n",
                        op, oldR, newR, Eobs, lower);
            }
        }
    }

    // 工具：取 Top/Bottom K
    private List<String> topKByImportance(boolean highFirst, double portion) {
        List<Map.Entry<String, Double>> list = new ArrayList<>(importanceMap.entrySet());
        list.sort((a, b) -> highFirst
                ? Double.compare(b.getValue(), a.getValue())
                : Double.compare(a.getValue(), b.getValue()));
        int k = Math.max(1, (int) (list.size() * portion));
        List<String> out = new ArrayList<>();
        for (int i = 0; i < Math.min(k, list.size()); i++) {
            out.add(list.get(i).getKey());
        }
        return out;
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

    public void printStats() {
        System.out.printf("[FAFT Stats] Processed=%d | Backups=%d | globalScale=%.2f%n",
                processedCount, backupCount, globalScale);
    }

}
