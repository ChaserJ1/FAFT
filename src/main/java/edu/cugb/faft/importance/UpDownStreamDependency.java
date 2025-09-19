package edu.cugb.faft.importance;

import java.util.*;

/**
 * 结构依赖度 D(v)：
 * 直观含义：v 能“触达”的下游节点规模与距离；距离越远影响衰减越多（alpha^dist）。
 * 用 BFS 计算 v 到其他节点的最短距离，累加 alpha^dist。
 */
public class UpDownStreamDependency {
    public static Map<String, Double> compute(Map<String, List<String>> dag, double alpha) {
        Map<String, Double> D = new HashMap<>();
        for (String v : dag.keySet()) {
            double score = 0.0;
            // BFS 计算到各节点的最短距离
            Deque<String> q = new ArrayDeque<>();
            Deque<Integer> dist = new ArrayDeque<>();
            q.add(v); dist.add(0);
            Set<String> seen = new HashSet<>(); seen.add(v);
            while (!q.isEmpty()) {
                String cur = q.poll();
                int d = dist.poll();
                if (d>0) score += Math.pow(alpha, d);
                for (String nxt : dag.getOrDefault(cur, Collections.emptyList())) {
                    if (!seen.contains(nxt)) {
                        seen.add(nxt);
                        q.add(nxt);
                        dist.add(d+1);
                    }
                }
            }
            D.put(v, score);
        }
        return D;
    }
}
