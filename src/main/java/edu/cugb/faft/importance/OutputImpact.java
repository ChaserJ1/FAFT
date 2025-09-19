package edu.cugb.faft.importance;

import java.util.*;

/**
 * 输出影响度 O(v)：
 * 直观含义：v 的失败对 Sink 的影响程度。采用反向拓扑传播 + 每跳衰减（delta）。
 * 简化设置：未提供边权时，默认每条边权重相等；Sink 的 O = 1，向上游传播时乘以 delta 累加。
 */
public class OutputImpact {

    /**
     * @param dag    有向无环图：op -> 下游列表
     * @param sinks  Sink 节点集合
     * @param delta  衰减系数（0,1]，越近影响越大
     */
    public static Map<String, Double> compute(Map<String, List<String>> dag, Set<String> sinks, double delta) {
        Map<String, Double> O = new HashMap<>();
        for (String v : dag.keySet()) O.put(v, 0.0);
        for (String s : sinks) O.put(s, 1.0);

        // 反向拓扑顺序传播：子 -> 父
        List<String> topo = topologicalOrder(dag);
        Collections.reverse(topo); // 反向传播

        Map<String, List<String>> parents = invert(dag);

        for (String v : topo) {
            // v 的影响，按父节点累加
            for (String p : parents.getOrDefault(v, Collections.emptyList())) {
                double contrib = delta * O.getOrDefault(v, 0.0);
                O.put(p, O.getOrDefault(p, 0.0) + contrib);
            }
        }
        return O;
    }

    private static Map<String, List<String>> invert(Map<String, List<String>> dag) {
        Map<String, List<String>> rev = new HashMap<>();
        for (Map.Entry<String, List<String>> e : dag.entrySet()) {
            for (String c : e.getValue()) {
                rev.computeIfAbsent(c, k -> new ArrayList<>()).add(e.getKey());
            }
        }
        return rev;
    }

    private static List<String> topologicalOrder(Map<String, List<String>> dag) {
        Map<String, Integer> indeg = new HashMap<>();
        for (String v : dag.keySet()) indeg.putIfAbsent(v, 0);
        for (List<String> outs : dag.values())
            for (String u : outs) indeg.put(u, indeg.getOrDefault(u, 0) + 1);

        Deque<String> q = new ArrayDeque<>();
        for (Map.Entry<String,Integer> e : indeg.entrySet()) if (e.getValue()==0) q.add(e.getKey());

        List<String> order = new ArrayList<>();
        while (!q.isEmpty()) {
            String v = q.poll();
            order.add(v);
            for (String u : dag.getOrDefault(v, Collections.emptyList())) {
                indeg.put(u, indeg.get(u)-1);
                if (indeg.get(u)==0) q.add(u);
            }
        }
        return order;
    }
}
