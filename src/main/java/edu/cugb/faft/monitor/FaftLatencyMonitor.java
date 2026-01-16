package edu.cugb.faft.monitor;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

/**
 * @Author: pengjia
 * @Description: 指标参数监控获取
 */
public class FaftLatencyMonitor {

    // zookeeper 客户端
    private static CuratorFramework client;
    // 故障时间读取路径
    private static final String FAILURE_PATH = "/faft/experiment/failure_time";
    // 恢复时间读取路径
    private static final String RECOVERY_PATH = "/faft/experiment/recovery_time";
    // 默认值，会被 setZkConnect 覆盖，zookeeper所在机器ip地址
    private static String ZK_CONN_STRING = "192.168.213.130:2181,192.168.213.131:2181,192.168.213.132:2181";
    // 尝试时间，做zk连不上的熔断降级
    private static long lastInitAttemptTime = 0;
    private static final long INIT_COOLDOWN_MS = 10000; // 10秒冷却

    public static synchronized void setZkConnect(String connectString) {
        if (connectString != null && !connectString.isEmpty()) {
            ZK_CONN_STRING = connectString;
        }
    }

    // 初始化 Zookeeper 客户端
    public static synchronized void init() {
        if (client != null) return;

        // 如果距离上次失败不到 10秒，直接跳过，不再尝试连接
        long now = System.currentTimeMillis();
        if (now - lastInitAttemptTime < INIT_COOLDOWN_MS) {
            return;
        }

        lastInitAttemptTime = now; // 更新尝试时间

        try {

            // 🔥 核心修改：设置极短的连接超时 (500ms)
            // 这样即使 ZK 没开，也只卡顿 0.5秒，不会导致 Storm 30秒超时
            client = CuratorFrameworkFactory.builder()
                    .connectString(ZK_CONN_STRING)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                    .connectionTimeoutMs(500) // 500ms 连不上就放弃
                    .sessionTimeoutMs(1000)
                    .build();

            client.start();
            // 立即检测，如果失败会抛出异常，进入 catch
            client.checkExists().forPath("/");
            /*client = CuratorFrameworkFactory.newClient(
                    ZK_CONN_STRING, new ExponentialBackoffRetry(1000, 3));
            client.start();
            // 简单测试一下连接，如果连不上直接抛异常，走 catch 逻辑
            client.checkExists().forPath("/");*/
        } catch (Throwable t) {
            System.err.println("❌ [Monitor] ZK 连接失败 (已放弃): " + t.getMessage());
            // 必须关闭资源，防止线程泄漏
            if (client != null) {
                try { client.close(); } catch (Exception ignored) {}
            }
            client = null;
        }
    }

    // 由ChaosBolt调用，记录故障时间
    public static void recordFailure() {
        try {
            init();
            if (client == null) return;

            long now = System.currentTimeMillis();

            if (client.checkExists().forPath(FAILURE_PATH) == null) {
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(FAILURE_PATH, String.valueOf(now).getBytes());
            } else {
                client.setData().forPath(FAILURE_PATH, String.valueOf(now).getBytes());
            }
            // 清除之前的恢复记录，为本次实验做准备
            try {
                client.delete().forPath(RECOVERY_PATH);
            } catch (Exception ignored) {
            }
            System.out.println("[EXP-METRIC] Faft 故障时间已记录：" + now);
        } catch (Throwable t) {
            System.err.println("⚠️ [Monitor] 记录故障时发生错误 (已忽略): " + t.getClass().getSimpleName() + ": " + t.getMessage());
        }
    }

    // 由SinkBolt 调用，记录恢复时间
    public static void checkAndRecordRecovery() {
        try {
            init();
            if (client == null) return;
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
                        System.out.println(">>>>>>>> [EXP-RESULT] Faft恢复耗时：: " + latency + " ms <<<<<<<<");
                    } catch (Exception ignored) {
                        // 节点已存在，说明其他并发线程（或其他Sink实例）已经记录了，忽略
                    }
                }
            }
        } catch (Throwable t) {
            // 这里不打印日志了，避免刷屏，静默失败即可
        }
    }

}
