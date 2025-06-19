package edu.cugb.faft.pojo;



/**
 * 该类用于存储算子的基本信息，包括算子id、CPU使用率和内存使用率
 */
public class OperatorInfo {

    public final String id;

    public final double cpuUsage;

    public final double memoryUsage;

    public OperatorInfo(String id, double cpuUsage, double memoryUsage) {
        this.id = id;
        this.cpuUsage = cpuUsage;
        this.memoryUsage = memoryUsage;
    }
}
