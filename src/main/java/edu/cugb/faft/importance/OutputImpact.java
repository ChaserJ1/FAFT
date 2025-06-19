package edu.cugb.faft.importance;

import java.util.*;

/**
 * @Author: pengjia
 * @Description:
 */
public class OutputImpact {
    /**
     * 计算所有节点的输出影响度（动态规划）
     * @param dag 有向图，键为节点ID，值为下游节点列表
     * @return 每个节点对应的输出影响度（影响下游 Sink 的程度）
     */
    public static Map<String, Integer> computeOutputImpact(Map<String, List<String>> dag) {
        Map<String, Integer> impactMap = new HashMap<>();
        Set<String> allNodes = new HashSet<>(dag.keySet());

        // 补充入所有出现在 value 列表里的节点（可能没有显式列为 key）
        for (List<String> children : dag.values()) {
            allNodes.addAll(children);
        }

        for (String node : allNodes) {
            dfs(node, dag, impactMap);
        }

        return impactMap;
    }

    /**
     * 动态规划 + 记忆化搜索方式计算某节点的输出影响度
     */
    private static int dfs(String node,
                           Map<String, List<String>> dag,
                           Map<String, Integer> memo) {
        if (memo.containsKey(node)) return memo.get(node);

        List<String> children = dag.getOrDefault(node, new ArrayList<>());
        if (children.isEmpty()) {
            // Sink 节点，最小影响度为 1
            memo.put(node, 1);
            return 1;
        }

        int impact = 0;
        for (String child : children) {
            impact += dfs(child, dag, memo);
        }

        memo.put(node, impact);
        return impact;
    }
}
