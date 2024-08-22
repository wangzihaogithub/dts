package com.github.dts.cluster;

import com.github.dts.util.JsonUtil;
import com.github.dts.util.Util;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;

public class SseStringBuilder extends StringWriter {
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private final JsonUtil.ObjectWriter objectWriter = JsonUtil.objectWriter();

    public void id(String id) {
        append("id:").append(id).append('\n');
    }

    public void name(String name) {
        append("event:").append(name).append('\n');
    }

    public void reconnectTime(long reconnectTimeMillis) {
        append("retry:").append(String.valueOf(reconnectTimeMillis)).append('\n');
    }

    public void comment(String comment) {
        append(':').append(comment).append('\n');
    }

    public void data(Object data) {
        append("data:");
        try {
            objectWriter.writeValue(this, data);
        } catch (IOException e) {
            Util.sneakyThrows(e);
        }
        append('\n').append('\n');
    }

    public byte[] build() {
        StringBuffer buffer = getBuffer();
        return buffer.toString().getBytes(UTF_8);
    }

    public void clear() {
        StringBuffer buffer = getBuffer();
        buffer.setLength(0);
        if (buffer.length() > 1024 * 1204) {
            buffer.trimToSize();
        }
    }


}