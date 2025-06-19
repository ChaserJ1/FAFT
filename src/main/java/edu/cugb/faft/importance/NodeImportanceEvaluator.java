package edu.cugb.faft.importance;



import edu.cugb.faft.pojo.OperatorInfo;

import java.util.*;

public class NodeImportanceEvaluator {

    private final double alpha;  // 输出影响度权重
    private final double beta;   // 上下游依赖权重
    private final double gamma;  // 计算复杂度权重

    public NodeImportanceEvaluator(double alpha, double beta, double gamma) {
        this.alpha = alpha;
        this.beta = beta;
        this.gamma = gamma;
    }

    /**
     * 计算所有节点的综合重要性评分
     */
    public Map<String, Double> computeImportance(Map<String, List<String>> dag,
                                                 Map<String, OperatorInfo> operatorResources) {
        // 1. 三个独立指标
        Map<String, Integer> outputImpact = OutputImpact.computeOutputImpact(dag);
        // 待实现
        Map<String, Double> dependencyWeight;
        Map<String, Double> complexityMap = new HashMap<>();

        for (Map.Entry<String, OperatorInfo> entry : operatorResources.entrySet()) {
            OperatorInfo info = entry.getValue();
            double complexity = ComputeComplexity.calculateComplexity(info.cpuUsage, info.memoryUsage);
            complexityMap.put(entry.getKey(), complexity);
        }

        // 2. 归一化（normalization）每个指标
        Map<String, Double> normImpact = normalize(outputImpact);
        Map<String, Double> normDep = normalize(null);
        Map<String, Double> normComplexity = normalize(complexityMap);

        // 3. 综合打分
        Map<String, Double> importanceMap = new HashMap<>();
        for (String node : dag.keySet()) {
            double imp = alpha * normImpact.getOrDefault(node, 0.0)
                       + beta  * normDep.getOrDefault(node, 0.0)
                       + gamma * normComplexity.getOrDefault(node, 0.0);
            importanceMap.put(node, imp);
        }

        return importanceMap;
    }

    /**
     * 将值映射到 0-1 范围
     */
    private Map<String, Double> normalize(Map<String, ? extends Number> raw) {
        double max = raw.values().stream().mapToDouble(Number::doubleValue).max().orElse(1.0);
        Map<String, Double> normalized = new HashMap<>();
        for (Map.Entry<String, ? extends Number> entry : raw.entrySet()) {
            normalized.put(entry.getKey(), entry.getValue().doubleValue() / max);
        }
        return normalized;
    }
}
