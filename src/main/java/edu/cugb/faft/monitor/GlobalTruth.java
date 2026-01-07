package edu.cugb.faft.monitor;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 本地模式专用的上帝视角数据共享类
 */
public class GlobalTruth {
    // 静态 Map，JVM 进程内共享
    private static final ConcurrentHashMap<String, Integer> REAL_COUNTS = new ConcurrentHashMap<>();

    // TrueCountBolt 调用：更新真值
    public static void update(String key) {
        REAL_COUNTS.merge(key, 1, Integer::sum);
    }

    // FaftSinkBolt 调用：获取真值
    public static int getRealCount(String key) {
        return REAL_COUNTS.getOrDefault(key, 0);
    }

    // 每次重启拓扑时清理旧数据
    public static void clear() {
        REAL_COUNTS.clear();
    }
}