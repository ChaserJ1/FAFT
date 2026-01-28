package edu.cugb.faft.manager;

import edu.cugb.faft.importance.NodeImportanceEvaluator;

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

    //单例模式
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

    // 叠加在“基准 r”上的全局缩放，用于误差闭环（让所有算子一起收/放），已改为局部调节（留作备用）
    private volatile double globalScale = 1.0;

    // 统计：全局/每算子处理计数（TPS 差分用）
    private long processedCount = 0L, backupCount = 0L;
    private final ConcurrentHashMap<String, Long> processedByOp = new ConcurrentHashMap<>();

    // 动态重算循环
    private final AtomicBoolean rebalanceStarted = new AtomicBoolean(false);

    // 模拟远程状态存储 (KV Store) - 本地调试使用。 todo：后续上集群需修改至redis
    private final ConcurrentHashMap<String, Map<String, Integer>> remoteStateStore = new ConcurrentHashMap<>();

    // 定期重算各个算子的重要性以及采样率
    private final ScheduledExecutorService rebalanceExec =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "faft-rebalancer");
                t.setDaemon(true);
                return t;
            });

    // 构造函数
    private ApproxBackupManager(double initRatio, double rmin, double rmax, double step) {
        this.currentRatio = initRatio;
        this.rmin = rmin;
        this.rmax = rmax;
        this.step = step;
    }

    // 用传来的采样率更新整张 per-op 采样率表
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

    /**
     * 尝试对指定算子进行备份操作
     *
     * @param operatorId 算子id，用于从表中查找对应的采样率
     * @param state 算子状态信息，当前未使用但保留用于后续扩展
     *//*
    public void tryBackup(String operatorId, Map<String, Integer> state) {
        // 更新采样计数器
        processedCount++;
        if (operatorId != null) processedByOp.merge(operatorId, 1L, Long::sum);

        // 根据算子id获取算子采样率
        double r = getRatioFor(operatorId);

        // 随机概率判断该算子是否需要做备份
        if (random.nextDouble() < r) {
            backupCount++;
            System.out.printf("[FAFT Backup][PER-OP] op=%s ratio=%.2f processed=%d backups=%d%n",
                    operatorId, r, processedCount, backupCount);
            // TODO: 真正的备份逻辑（落盘/远端等）
        }
    }*/

    /**
     * 具体的备份逻辑 (FaftCountBolt 调用)
     * @param operatorId 算子ID
     * @param word       单词
     * @param count      计数值
     */
    public void tryBackup(String operatorId, String word, int count) {
        processedCount++;
        if (operatorId != null) processedByOp.merge(operatorId, 1L, Long::sum);

        double r = getRatioFor(operatorId);

        // 采样判断
        if (random.nextDouble() < r) {
            backupCount++;
            // 存入模拟的远程存储
            remoteStateStore.computeIfAbsent(operatorId, k -> new ConcurrentHashMap<>())
                    .put(word, count);
        }
    }

    /**
     * 获取备份 (用于故障恢复)
     */
    public Map<String, Integer> getBackup(String operatorId) {
        Map<String, Integer> data = remoteStateStore.get(operatorId);
        if (data == null) return new HashMap<>();
        // 返回拷贝，防止并发修改
        return new HashMap<>(data);
    }


    // 根据算子 id，计算算子采样率 r
    private double getRatioFor(String operatorId) {
        // 从算子采样率基准表中获取算子基准采样率
        double base = ratioByOperator.getOrDefault(operatorId, currentRatio);
        // 乘以全局缩放因子
        return clamp(base * globalScale);
    }

    // 限制采样率范围
    private double clamp(double x) { return Math.max(rmin, Math.min(rmax, x)); }

    /**
     * 根据 Sink 端测得的误差进行反馈调节
     * @param Eobs 观测误差
     * @param Emax 允许的最大误差阈值
     */
    public synchronized void adjustByError(double Eobs, double Emax) {
        double lower = 0.5 * Emax;      // 滞回下界

        // 1. 获取所有算子条目，准备排序
        List<Map.Entry<String, Double>> sortedOps = new ArrayList<>(importanceMap.entrySet());

        // 2. 计算本轮目标调整多少个算子
        int targetCount = Math.max(1, (int) (sortedOps.size() * 0.3));
        int adjustedCount = 0;

        if (Eobs > Emax) { // 误差过大，执行上调

            sortedOps.sort((a, b) -> Double.compare(b.getValue(), a.getValue())); // 降序排列

            for (Map.Entry<String, Double> entry : sortedOps) {
                String op = entry.getKey();
                double oldR = ratioByOperator.getOrDefault(op, currentRatio);

                // 如果该算子已经满载 (接近 1.0)，则跳过
                // 避免死锁在 split-bolt 上，让机会流向 chaos-bolt 等低采样节点
                if (oldR >= rmax - 0.001) {
                    continue;
                }

                // 执行上调
                double newR = clamp(oldR + step);
                ratioByOperator.put(op, newR);

                System.out.printf("[FAFT Adjust][LOCAL-UP] op=%s oldR=%.3f newR=%.3f (error=%.4f > %.4f)%n",
                        op, oldR, newR, Eobs, Emax);

                adjustedCount++;
                // 如果已经调够了目标数量，就结束本轮调节
                if (adjustedCount >= targetCount) break;
            }

            if (adjustedCount == 0) {
                System.out.println("[FAFT Adjust] ⚠️ 警告：系统全员满载(1.0)，无法进一步降低误差！");
            }

        } else if (Eobs < lower) { // 误差过小，执行下调

            // 策略：按重要性从低到高排序 (优先牺牲不重要的节点)
            sortedOps.sort((a, b) -> Double.compare(a.getValue(), b.getValue())); // 升序

            for (Map.Entry<String, Double> entry : sortedOps) {
                String op = entry.getKey();
                double oldR = ratioByOperator.getOrDefault(op, currentRatio);

                // 如果该算子已经触底 (接近 0.1)，则跳过
                if (oldR <= rmin + 0.001) {
                    continue;
                }

                // 执行下调
                double newR = clamp(oldR - step);
                ratioByOperator.put(op, newR);

                System.out.printf("[FAFT Adjust][LOCAL-DOWN] op=%s oldR=%.3f newR=%.3f (error=%.4f < %.4f)%n",
                        op, oldR, newR, Eobs, lower);

                adjustedCount++;
                if (adjustedCount >= targetCount) break;
            }
        }
    }


    /**
     * 获得 Top/Bottom K 算子列表
     * @param highFirst 低分(false)/高分(true)优先
     * @param portion   选取比例
     * @return 算子列表
     */
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

    // ====== Step4：动态重算 I→r（每 periodMs 刷新一次）====== 暂时为10s
    public void startDynamicRebalance(Map<String, List<String>> dag,
                                      Set<String> sinks,
                                      Map<String, NodeImportanceEvaluator.Weights> weightsMap,
                                      NodeImportanceEvaluator.Weights defaultWeights,
                                      double impactDelta, double decayAlpha,
                                      double rmin, double rmax,
                                      long periodMs) {
        if (!rebalanceStarted.compareAndSet(false, true)) return; // 避免重复启动

        final Map<String, Long> lastCount = new ConcurrentHashMap<>();
        rebalanceExec.scheduleAtFixedRate(() -> {
            try {
                // A) TPS 计算每算子的相对处理速度
                Map<String, Double> tps = new HashMap<>();
                for (String op : dag.keySet()) {
                    long now = processedByOp.getOrDefault(op, 0L);
                    long prev = lastCount.getOrDefault(op, 0L);
                    double rate = Math.max(0, now - prev) / (periodMs / 1000.0);
                    tps.put(op, rate);
                    lastCount.put(op, now);
                }
                double maxTps = tps.values().stream().mapToDouble(x -> x).max().orElse(1.0);

                // B) CPU / Mem 获取当前 JVM 进程的负载作为近似值
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

                // D) 调用评估器重算 I→r
                NodeImportanceEvaluator.Result res =
                        NodeImportanceEvaluator.evaluateAndAssignRatios(
                                dag, sinks, infos, weightsMap, defaultWeights, impactDelta, decayAlpha, rmin, rmax);

                // E) 刷新 per-op 基准 r 表, 同步刷新 importance 表
                updateSamplingRatios(res.R);
                // System.out.println("[FAFT Rebalance] R=" + res.R);
                System.out.println("采样率已更新");
                updateImportance(res.I);
                System.out.println("重要性已更新");
                // System.out.println("[FAFT Rebalance] I=" + res.I);
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
