package edu.cugb.faft.importance;

import java.util.*;

/**
 * @Author: pengjia
 * @Description:
 */
public class test {
    public static void main(String[] args) {
        Map<String, List<String>> dag = new LinkedHashMap<>();
        dag.put("Source1", List.of("A"));
        dag.put("A", List.of("B", "C"));
        dag.put("B", List.of("Sink"));
        dag.put("C", List.of("D"));
        dag.put("D", List.of("Sink"));
        dag.put("Sink", new ArrayList<>());

        UpDownStreamDependency evaluator = new UpDownStreamDependency(dag);
        evaluator.evaluate();
        Map<String, Integer> impactMap = OutputImpact.computeOutputImpact(dag);
        for (Map.Entry<String, Integer> entry : impactMap.entrySet()) {
            System.out.printf("Node: %-10s | OutputImpact: %d%n", entry.getKey(), entry.getValue());
        }
    }
}
