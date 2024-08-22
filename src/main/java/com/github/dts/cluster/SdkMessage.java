package com.github.dts.cluster;

public class SdkMessage {
    private static final ThreadLocal<SseStringBuilder> sseStringBuilderThreadLocal = ThreadLocal.withInitial(SseStringBuilder::new);
    public final MessageTypeEnum messageTypeEnum;
    public final Object payload;
    public Long id;
    private volatile byte[] sseBytes;

    public SdkMessage(MessageTypeEnum messageTypeEnum, Object payload) {
        this.messageTypeEnum = messageTypeEnum;
        this.payload = payload;
    }

    public byte[] toSseBytes() {
        if (sseBytes == null) {
            synchronized (this) {
                if (sseBytes == null) {
                    SseStringBuilder builder = sseStringBuilderThreadLocal.get();
                    try {
                        if (id != null) {
                            builder.id(id.toString());
                        }
                        builder.name(messageTypeEnum.getType());
                        builder.data(payload);
                        sseBytes = builder.build();
                    } finally {
                        builder.clear();
                    }
                }
            }
        }
        return sseBytes;
    }
}
