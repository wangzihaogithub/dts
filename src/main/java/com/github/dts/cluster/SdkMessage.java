package com.github.dts.cluster;

public class SdkMessage {
    private static final ThreadLocal<SseStringBuilder> sseStringBuilderThreadLocal = ThreadLocal.withInitial(SseStringBuilder::new);
    public final MessageTypeEnum messageTypeEnum;
    public final Object payload;
    public final Long id;
    private byte[] sseBytes;

    public SdkMessage(Long id, MessageTypeEnum messageTypeEnum, Object payload) {
        this.id = id;
        this.messageTypeEnum = messageTypeEnum;
        this.payload = payload;
    }

    public byte[] toSseBytes() {
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
        return sseBytes;
    }
}
