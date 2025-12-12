package edu.cugb.faft.monitor;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * @Author: pengjia
 * @Description: 指标参数监控获取
 */
public class DistributedLatencyMonitor {

    // zookeeper 客户端
    private static CuratorFramework client;
    // 故障时间读取路径
    private static final String FAILURE_PATH = "/faft/experiment/failure_time";
    // 恢复时间读取路径
    private static final String RECOVERY_PATH = "/faft/experiment/recovery_time";
    //zookeeper所在机器ip地址
    private static final String ZK_CONN_STRING = "192.168.213.130:2181";


    // 初始化 Zookeeper 客户端
    public static synchronized void init() {
        if (client == null) {
            CuratorFrameworkFactory.newClient(
                    ZK_CONN_STRING, new ExponentialBackoffRetry(1000, 3));
            client.start();
        }
    }

    // 由ChaosBolt调用，记录故障时间
    public static void recordFailure() {
        try {
            init();
            long now = System.currentTimeMillis();
            if (client.checkExists().forPath(FAILURE_PATH) == null) {
                client.create().creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(FAILURE_PATH, String.valueOf(now).getBytes());
            } else {
                client.setData().forPath(FAILURE_PATH, String.valueOf(now).getBytes());
            }
            // 清除之前的恢复记录，为本次实验做准备
            if (client.checkExists().forPath(RECOVERY_PATH) != null) {
                client.delete().forPath(RECOVERY_PATH);
            }
            System.out.println("[EXP-METRIC] Distributed Failure recorded at: " + now);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 由SinkBolt 调用，记录恢复时间
    public static void checkAndRecordRecovery() {
        try {
            init();
            // 只有当检测到有“故障发生”且“尚未记录恢复”时，才记录当前时间
            if (client.checkExists().forPath(FAILURE_PATH) != null &&
                    client.checkExists().forPath(RECOVERY_PATH) == null) {

                long now = System.currentTimeMillis();
                byte[] failureData = client.getData().forPath(FAILURE_PATH);
                long failTime = Long.parseLong(new String(failureData));

                // 防抖动：只有故障发生 100ms 后才算有效恢复，避免网络残余包
                if (now - failTime > 100) {
                    // 利用 Zookeeper 的原子性创建节点，只有第一个成功的线程能记录恢复时间（避免并发）
                    try {
                        client.create().withMode(CreateMode.PERSISTENT).forPath(RECOVERY_PATH, String.valueOf(now).getBytes());
                        long latency = now - failTime;
                        System.out.println(">>>>>>>> [EXP-RESULT] RECOVERY LATENCY: " + latency + " ms <<<<<<<<");
                    } catch (Exception ignored) {
                        // 节点已存在，说明其他并发线程（或其他Sink实例）已经记录了，忽略
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
