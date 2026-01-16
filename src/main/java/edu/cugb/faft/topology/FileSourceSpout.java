package edu.cugb.faft.topology;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class FileSourceSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private RandomAccessFile raf;
    private String fileName;
    private boolean loop;

    // 重发队列：存储处理失败需要重发的 offset, 保基准流的可靠性
    private LinkedBlockingQueue<Long> replayQueue;

    public FileSourceSpout(String fileName, boolean loop) {
        this.fileName = fileName;
        this.loop = loop;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.replayQueue = new LinkedBlockingQueue<>();
        try {
            // 这里兼容一下，防止路径拼接出错
            File f = new File(fileName);
            String absolutePath = f.isAbsolute() ? fileName : System.getProperty("user.dir") + File.separator + fileName;

            System.out.println(">> [FileSpout] 打开数据文件成功，文件地址为： " + absolutePath);
            this.raf = new RandomAccessFile(absolutePath, "r");
        } catch (Exception e) {
            throw new RuntimeException("无法打开数据文件: " + fileName, e);
        }
    }

    @Override
    public void nextTuple() {
        try {
            // 1. 优先重发失败的数据 (保证基准流不丢数据)
            Long replayOffset = replayQueue.poll();
            if (replayOffset != null) {
                sendLineAtOffset(replayOffset);
                return;
            }

            // 2. 正常读取下一行
            long currentOffset = raf.getFilePointer();
            String line = raf.readLine();

            if (line != null && !line.trim().isEmpty()) {
                // 发射整行数据，附带 offset 作为 msgId
                collector.emit(new Values(line), currentOffset);

                // 限流控制：每发送一条睡 1 毫秒
                // 将 TPS 限制在 1000 左右，防止本地模式下瞬间吞吐量过大导致内存溢出(OOM)或系统崩溃
                try { Thread.sleep(1); } catch (InterruptedException e) {}
            } else {
                // 读到文件末尾
                if (loop) {
                    raf.seek(0); // 循环读取，维持压力
                } else {
                    // 如果不循环，稍微休息一下避免空转 CPU 100%+
                    Thread.sleep(10);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 回溯文件指针进行重发
    private void sendLineAtOffset(long offset) throws IOException {
        try {
            long originalPos = raf.getFilePointer(); // 记录当前读到的位置
            raf.seek(offset);
            String line = raf.readLine();
            if (line != null) {
                // System.out.println("🔄 [Replay] Offset: " + offset);
                collector.emit(new Values(line), offset);
            }
            raf.seek(originalPos); // 恢复到原来的位置继续读
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object msgId) { /* 成功不处理 */ }

    @Override
    public void fail(Object msgId) {
        // 只有当基准流真的处理失败，或者 ChaosBolt 决定不欺骗而是真的让 Spout 重发时会触发
        // 在目前的双轨制设计中，这主要用于保障基准流的绝对可靠性
        if (msgId instanceof Long) {
            replayQueue.offer((Long) msgId); // // 加入重发队列
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    @Override
    public void close() {
        try { if (raf != null) raf.close(); } catch (IOException e) {}
    }
}