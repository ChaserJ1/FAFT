package edu.cugb.faft.importance;

import java.util.*;

/**
 * 节点的上下游依赖关系评估
 * 通过多种指标综合计算节点重要性：
 * 1. 上游节点数量 - 指向当前节点的节点数量
 * 2. 下游节点数量 - 当前节点指向的节点数量
 * 3. 可达性大小 - 从当前节点出发可到达的不同节点数量
 * 4. 总路径长度 - 从当前节点出发到所有可达节点的路径长度之和
 * 5. 衰减影响分数 - 考虑路径长度衰减的影响范围评分
 */
public class UpDownStreamDependency {
    private Map<String, List<String>> graph;           // 存储图的邻接表
    private Map<String, Integer> upstreamCount;        // 每个节点的上游节点数量
    private Map<String, Integer> downstreamCount;      // 每个节点的下游节点数量
    private Map<String, Integer> reachabilitySize;     // 每个节点的可达节点数量
    private Map<String, Integer> totalPathLength;      // 每个节点的总路径长度
    private Map<String, Double> decayImpactScore;      // 每个节点的衰减影响分数

    private final double alpha = 0.9; // 衰减因子，控制远距离影响的衰减程度

    /**
     * 构造函数，初始化图结构
     * @param graph 图的邻接表表示，键为节点ID，值为该节点指向的节点列表
     */
    public UpDownStreamDependency(Map<String, List<String>> graph) {
        this.graph = graph;
    }

    public UpDownStreamDependency() {

    }

    /**
     * 执行节点重要性评估的主方法
     * 按顺序执行各项评估指标的计算，并输出结果
     */
    public void evaluate() {
        initializeUpstreamDownstreamCounts();  // 初始化上下游节点数量
        computeReachabilityAndPathLength();    // 计算可达性和总路径长度
        computeDecayImpactScores();            // 计算衰减影响分数
        printNodeImportanceScores();           // 输出节点重要性评分结果
    }

    /**
     * 初始化每个节点的上游和下游节点数量
     * 上游节点数量表示有多少节点指向当前节点
     * 下游节点数量表示当前节点指向多少个节点
     */
    private void initializeUpstreamDownstreamCounts() {
        upstreamCount = new HashMap<>();
        downstreamCount = new HashMap<>();

        // 遍历图中每个节点及其下游节点，统计上下游数量
        for (String node : graph.keySet()) {
            downstreamCount.put(node, graph.get(node).size());
            for (String downstream : graph.get(node)) {
                upstreamCount.put(downstream, upstreamCount.getOrDefault(downstream, 0) + 1);
            }
        }

        // 确保所有节点都在上下游映射中存在，即使值为0
        for (String node : graph.keySet()) {
            upstreamCount.putIfAbsent(node, 0);
            downstreamCount.putIfAbsent(node, 0);
        }
    }

    /**
     * 计算每个节点的可达性大小和总路径长度
     * 可达性大小：从当前节点出发可到达的不同节点数量
     * 总路径长度：从当前节点出发到所有可达节点的路径长度之和
     * 使用广度优先搜索(BFS)算法遍历图
     */
    private void computeReachabilityAndPathLength() {
        reachabilitySize = new HashMap<>();
        totalPathLength = new HashMap<>();

        // 对每个节点进行BFS遍历
        for (String node : graph.keySet()) {
            Set<String> visited = new HashSet<>();  // 记录已访问节点
            Queue<Map.Entry<String, Integer>> queue = new LinkedList<>();  // BFS队列，存储节点及其当前路径长度
            queue.add(Map.entry(node, 0));  // 从当前节点出发，初始路径长度为0

            int count = 0;  // 可达节点计数
            int totalLength = 0;  // 总路径长度

            // BFS核心逻辑
            while (!queue.isEmpty()) {
                Map.Entry<String, Integer> current = queue.poll();
                String currentNode = current.getKey();
                int depth = current.getValue();

                // 如果是可达的新节点（非自身），则更新计数和总路径长度
                if (!visited.contains(currentNode) && !currentNode.equals(node)) {
                    visited.add(currentNode);
                    count++;
                    totalLength += depth;
                }

                // 将未访问的邻居节点加入队列
                for (String neighbor : graph.getOrDefault(currentNode, new ArrayList<>())) {
                    if (!visited.contains(neighbor)) {
                        queue.add(Map.entry(neighbor, depth + 1));
                    }
                }
            }

            // 存储当前节点的可达性和总路径长度
            reachabilitySize.put(node, count);
            totalPathLength.put(node, totalLength);
        }
    }

    /**
     * 计算每个节点的衰减影响分数
     * 考虑路径长度的衰减效应，距离越远影响越小
     * 衰减公式：impact = α^depth，其中α是衰减因子，depth是路径长度
     */
    private void computeDecayImpactScores() {
        decayImpactScore = new HashMap<>();

        // 对每个节点进行BFS遍历计算衰减影响
        for (String node : graph.keySet()) {
            double score = 0.0;  // 初始分数
            Set<String> visited = new HashSet<>();  // 记录已访问节点
            Queue<Map.Entry<String, Integer>> queue = new LinkedList<>();  // BFS队列，存储节点及其当前路径长度
            queue.add(Map.entry(node, 1));  // 从当前节点出发，初始路径长度为1（自身影响为α^1）

            // BFS核心逻辑
            while (!queue.isEmpty()) {
                Map.Entry<String, Integer> current = queue.poll();
                String currentNode = current.getKey();
                int depth = current.getValue();

                // 计算当前节点的衰减影响并累加到总分
                if (!visited.contains(currentNode)) {
                    visited.add(currentNode);
                    score += Math.pow(alpha, depth);  // 衰减公式：α^depth
                    // 将未访问的邻居节点加入队列
                    for (String neighbor : graph.getOrDefault(currentNode, new ArrayList<>())) {
                        queue.add(Map.entry(neighbor, depth + 1));
                    }
                }
            }

            // 存储当前节点的衰减影响分数
            decayImpactScore.put(node, score);
        }
    }

    /**
     * 打印所有节点的重要性评分结果
     * 包括：上游节点数、下游节点数、可达性大小、总路径长度和衰减影响分数
     */
    private void printNodeImportanceScores() {
        System.out.println("Node USDependency Scores:");
        for (String node : graph.keySet()) {
            System.out.printf("Node: %-10s | Up: %2d | Down: %2d | Reach: %2d | PathSum: %2d | DecayImpact: %.3f%n",
                    node,
                    upstreamCount.get(node),
                    downstreamCount.get(node),
                    reachabilitySize.get(node),
                    totalPathLength.get(node),
                    decayImpactScore.get(node));
        }
    }
}