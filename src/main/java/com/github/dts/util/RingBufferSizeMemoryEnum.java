package com.github.dts.util;

public enum RingBufferSizeMemoryEnum {
    MEMORY_64KB(8),
    MEMORY_256KB(16),
    MEMORY_1MB(32),
    MEMORY_4MB(64),
    MEMORY_16MB(128),
    MEMORY_64MB(256),
    MEMORY_256MB(512),
    MEMORY_1G(1024),
    MEMORY_4G(2048),
    MEMORY_16G(4096),
    MEMORY_64G(8192),
    MEMORY_256G(16384);
    private final int bufferSizeKB;
    private final int memoryKB;

    RingBufferSizeMemoryEnum(int bufferSizeKB) {
        this.bufferSizeKB = bufferSizeKB;
        this.memoryKB = bufferSizeKB * bufferSizeKB;
    }

    public static RingBufferSizeMemoryEnum getByJvmMaxMemoryRate(int rate) {
        long jvmMaxMemory = Runtime.getRuntime().maxMemory();
        double jvmMaxMemoryKB = jvmMaxMemory / 1024D;
        int usedMemoryKB = (int) (jvmMaxMemoryKB * (rate / 100D));

        RingBufferSizeMemoryEnum[] values = values();
        RingBufferSizeMemoryEnum lo = values[0];
        RingBufferSizeMemoryEnum hi = values[0];
        for (RingBufferSizeMemoryEnum value : values) {
            if (value.memoryKB > usedMemoryKB) {
                hi = value;
                break;
            }
            lo = value;
        }
        int diffHi = hi.memoryKB - usedMemoryKB;
        int diffLo = usedMemoryKB - lo.memoryKB;
        RingBufferSizeMemoryEnum result = diffLo < diffHi ? lo : hi;

        RingBufferSizeMemoryEnum min = RingBufferSizeMemoryEnum.MEMORY_1G;
        if (result.getBufferSizeKB() < min.getBufferSizeKB() && jvmMaxMemory > min.getBufferSizeKB()) {
            return min;
        } else {
            return result;
        }
    }

    public int getBufferSizeKB() {
        return bufferSizeKB;
    }

    public int getMemoryKB() {
        return memoryKB;
    }
}
