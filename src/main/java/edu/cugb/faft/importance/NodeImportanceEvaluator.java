package edu.cugb.faft.importance;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 计算 I(v) = α*O(v) + β*D(v) + γ*C(v)
 * 并按 I(v) 归一映射到初始采样率 r(v) ∈ [rmin, rmax]
 */
public class NodeImportanceEvaluator {

    public static class Result {
        public final Map<String, Double> O, D, C; // 3个子指标(已归一化)
        public final Map<String, Double> I;  // 重要性
        public final Map<String, Double> R;  // 初始采样率
        public final double maxI;

        public Result(Map<String, Double> O, Map<String, Double> D, Map<String, Double> C,
                      Map<String, Double> I, Map<String, Double> R, double maxI) {
            this.O = O;
            this.D = D;
            this.C = C;
            this.I = I;
            this.R = R;
            this.maxI = maxI;
        }
    }

    public static Result evaluateAndAssignRatios(
            Map<String, List<String>> dag,              // DAG
            Set<String> sinks,                          // Sink 集合
            Map<String, OperatorInfo> opInfo,           // 算子资源/吞吐信息
            double alpha, double beta, double gamma,    // I(v)的三个权重系数
            double delta,                               // O(v) 衰减参数
            double decayAlpha,                          // D(v) 衰减参数
            double rmin, double rmax)                   // 采样率上下限
    {

        // 1) O(v)
        Map<String, Double> O = OutputImpact.compute(dag, sinks, delta);
        O = normalize(O);

        // 2) D(v)
        Map<String, Double> D = UpDownStreamDependency.compute(dag, decayAlpha);
        D = normalize(D);

        // 3) C(v)（未归一）→ 归一（若 opInfo 缺失则默认 0）
        Map<String, Double> Craw = new HashMap<>();
        for (String v : dag.keySet()) {
            OperatorInfo oi = opInfo.get(v);
            double c = (oi == null) ? 0.0 : ComputeComplexity.compute(oi.cpuUsage, oi.memUsage, oi.tps,
                    0.5, 0.3, 0.2);
            Craw.put(v, c);
        }
        Map<String, Double> C = normalize(Craw);


        // 4) I(v)
        Map<String, Double> I = new HashMap<>();
        double maxI = 0.0;
        for (String v : dag.keySet()) {
            double iv = alpha * O.getOrDefault(v, 0.0)
                    + beta * D.getOrDefault(v, 0.0)
                    + gamma * C.getOrDefault(v, 0.0);
            I.put(v, iv);
            maxI = Math.max(maxI, iv);
        }

        // 6) r(v)
        Map<String, Double> R = new HashMap<>();
        for (String v : dag.keySet()) {
            double rv = (maxI == 0.0) ? rmin : rmin + (I.get(v) / maxI) * (rmax - rmin);
            R.put(v, rv);
        }
        return new Result(O, D, C, I, R, maxI);
    }

    private static Map<String, Double> normalize(Map<String, Double> m) {
        double max = 0.0;
        for (double v : m.values()) max = Math.max(max, v);
        if (max == 0.0) return new HashMap<>(m);
        Map<String, Double> out = new HashMap<>();
        for (Map.Entry<String, Double> e : m.entrySet()) out.put(e.getKey(), e.getValue() / max);
        return out;
    }
}
