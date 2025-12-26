package edu.cugb.faft.monitor;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * @Author: pengjia
 * @Description: 指标参数监控获取
 */
public class DefaultLatencyMonitor {

    // zookeeper 客户端
    private static CuratorFramework client;
    // 故障时间读取路径
    private static final String FAILURE_PATH = "/default/experiment/failure_time";
    // 恢复时间读取路径
    private static final String RECOVERY_PATH = "/default/experiment/recovery_time";
    //zookeeper所在机器ip地址
    private static String ZK_CONN_STRING = "192.168.213.130:2181";


    public static synchronized void setZkConnect(String connectString) {
        if (connectString != null && !connectString.isEmpty()) {
            ZK_CONN_STRING = connectString;
        }
    }

    // 初始化 Zookeeper 客户端
    public static synchronized void init() {
        if (client == null) {
            client = CuratorFrameworkFactory.newClient(
                    ZK_CONN_STRING, new ExponentialBackoffRetry(1000, 3));
            client.start();
        }
    }

    // 1. 记录故障时间，在ChaosBolt抛异常前调用
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
            // 清除之前的恢复记录，为本次故障做准备
            if (client.checkExists().forPath(RECOVERY_PATH) != null) {
                client.delete().forPath(RECOVERY_PATH);
            }
            System.out.println("[EXP-METRIC] Default failure has benn recorded");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 由 SinkBolt 调用，记录恢复时间
    public static void checkAndRecordRecovery() {
        try {
            init();
            // 只有当检测到有故障发生且无恢复记录时，才记录当前时间
            if (client.checkExists().forPath(FAILURE_PATH) != null &&
                    client.checkExists().forPath(RECOVERY_PATH) == null) {

                long now = System.currentTimeMillis();
                byte[] failureData = client.getData().forPath(FAILURE_PATH);
                long failTime = Long.parseLong(new String(failureData));

                // 防抖动：Storm 默认恢复很慢，如果时间差太短(比如50ms)，可能是故障前就在传输的数据，忽略它
                if (now - failTime > 100) {
                    // 利用 Zookeeper 的原子性创建节点，只有第一个成功的线程能记录恢复时间（避免并发）
                    try {
                        client.create().withMode(CreateMode.PERSISTENT).forPath(RECOVERY_PATH, String.valueOf(now).getBytes());

                        long latency = now - failTime;

                        System.out.println(">>>>>>>> [EXP-RESULT] Storm默认机制恢复耗时: " + latency + " ms <<<<<<<<");
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
