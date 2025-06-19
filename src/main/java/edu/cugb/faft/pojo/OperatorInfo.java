package edu.cugb.faft.pojo;

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
