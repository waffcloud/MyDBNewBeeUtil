package com.cetc.core.util;

public class IdGenerator {
    private final static long TWEPOCH = 1288834974657L;
    private final static long WORKER_ID_BITS = 5L;                                                         // 机器标识位数
    private final static long DATA_CENTER_ID_BITS = 5L;                                                    // 数据中心标识位数
    private final static long MAX_WORKER_ID = -1L ^ (-1L << WORKER_ID_BITS);                               // 机器ID最大值
    private final static long MAX_DATA_CENTER_ID = -1L ^ (-1L << DATA_CENTER_ID_BITS);                     // 数据中心ID最大值
    private final static long SEQUENCE_BITS = 12L;                                                         // 毫秒内自增位
    private final static long WORKER_ID_SHIFT = SEQUENCE_BITS;                                             // 机器ID偏左移12位
    private final static long DATA_CENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;                       // 数据中心ID左移17位
    private final static long TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATA_CENTER_ID_BITS; // 时间毫秒左移22位
    private final static long SEQUENCE_MASK = -1L ^ (-1L << SEQUENCE_BITS);

    private static long lastTimestamp = -1L;
    private long sequence = 0L;
    private final long workerId;
    private final long dataCenterId;

    public IdGenerator(long workerId, long dataCenterId) {
        if (workerId > MAX_WORKER_ID || workerId < 0) {
            throw new IllegalArgumentException("worker Id can't be greater than %d or less than 0");
        }
        if (dataCenterId > MAX_DATA_CENTER_ID || dataCenterId < 0) {
            throw new IllegalArgumentException("data center Id can't be greater than %d or less than 0");
        }
        this.workerId = workerId;
        this.dataCenterId = dataCenterId;
    }

    /**
     *  算法生成的全系唯一UUID
     *  nextId()
     *  @return long UUId
     *  @throws Exception
     *  @author zhangliang
     * */
    public synchronized long nextId() {
        long timestamp = timeGen();
        if (timestamp < lastTimestamp) {
            try {
                throw new Exception("Clock moved backwards.  Refusing to generate id for "+ (lastTimestamp - timestamp) + " milliseconds");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;  /* 当前毫秒内，则+1 */
            if (sequence == 0) {
                timestamp = tilNextMillis(lastTimestamp); /* 当前毫秒内计数满了，则等待下一秒 */
            }
        }
        else
            { sequence = 0;}
        lastTimestamp = timestamp;

        // ID偏移组合生成最终的ID，并返回ID
        return  ((timestamp - TWEPOCH) << TIMESTAMP_LEFT_SHIFT)
                | (dataCenterId << DATA_CENTER_ID_SHIFT)
                | (workerId << WORKER_ID_SHIFT) | sequence;
    }

    private long tilNextMillis(final long lastTimestamp) {
        long timestamp = this.timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = this.timeGen();
        }
        return timestamp;
    }

    private long timeGen() {
        return System.currentTimeMillis();
    }
}