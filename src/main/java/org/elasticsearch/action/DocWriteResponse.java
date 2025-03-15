//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.elasticsearch.action;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Locale;
import java.util.Objects;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

public abstract class DocWriteResponse extends ReplicationResponse implements WriteResponse, StatusToXContentObject {
    private static final String _SHARDS = "_shards";
    private static final String _INDEX = "_index";
    private static final String _TYPE = "_type";
    private static final String _ID = "_id";
    private static final String _VERSION = "_version";
    private static final String _SEQ_NO = "_seq_no";
    private static final String _PRIMARY_TERM = "_primary_term";
    private static final String RESULT = "result";
    private static final String FORCED_REFRESH = "forced_refresh";
    private final ShardId shardId;
    private final String id;
    private final String type;
    private final long version;
    private final long seqNo;
    private final long primaryTerm;
    private boolean forcedRefresh;
    protected final Result result;

    public DocWriteResponse(ShardId shardId, String type, String id, long seqNo, long primaryTerm, long version, Result result) {
        this.shardId = (ShardId)Objects.requireNonNull(shardId);
        this.type = type == null ? "_doc" : type;// wangzihaogithub，es8没有type字段了
        this.id = (String)Objects.requireNonNull(id);
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.version = version;
        this.result = (Result)Objects.requireNonNull(result);
    }

    protected DocWriteResponse(ShardId shardId, StreamInput in) throws IOException {
        super(in);
        this.shardId = shardId;
        this.type = in.readString();
        this.id = in.readString();
        this.version = in.readZLong();
        this.seqNo = in.readZLong();
        this.primaryTerm = in.readVLong();
        this.forcedRefresh = in.readBoolean();
        this.result = DocWriteResponse.Result.readFrom(in);
    }

    protected DocWriteResponse(StreamInput in) throws IOException {
        super(in);
        this.shardId = new ShardId(in);
        this.type = in.readString();
        this.id = in.readString();
        this.version = in.readZLong();
        if (in.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            this.seqNo = in.readZLong();
            this.primaryTerm = in.readVLong();
        } else {
            this.seqNo = -2L;
            this.primaryTerm = 0L;
        }

        this.forcedRefresh = in.readBoolean();
        this.result = DocWriteResponse.Result.readFrom(in);
    }

    public Result getResult() {
        return this.result;
    }

    public String getIndex() {
        return this.shardId.getIndexName();
    }

    public ShardId getShardId() {
        return this.shardId;
    }

    @Deprecated
    public String getType() {
        return this.type;
    }

    public String getId() {
        return this.id;
    }

    public long getVersion() {
        return this.version;
    }

    public long getSeqNo() {
        return this.seqNo;
    }

    public long getPrimaryTerm() {
        return this.primaryTerm;
    }

    public boolean forcedRefresh() {
        return this.forcedRefresh;
    }

    public void setForcedRefresh(boolean forcedRefresh) {
        this.forcedRefresh = forcedRefresh;
    }

    public RestStatus status() {
        return this.getShardInfo().status();
    }

    public String getLocation(@Nullable String routing) {
        String encodedIndex;
        String encodedType;
        String encodedId;
        String encodedRouting;
        try {
            encodedIndex = URLEncoder.encode(this.getIndex(), "UTF-8");
            encodedType = URLEncoder.encode(this.getType(), "UTF-8");
            encodedId = URLEncoder.encode(this.getId(), "UTF-8");
            encodedRouting = routing == null ? null : URLEncoder.encode(routing, "UTF-8");
        } catch (UnsupportedEncodingException var10) {
            UnsupportedEncodingException e = var10;
            throw new AssertionError(e);
        }

        String routingStart = "?routing=";
        int bufferSizeExcludingRouting = 3 + encodedIndex.length() + encodedType.length() + encodedId.length();
        int bufferSize;
        if (encodedRouting == null) {
            bufferSize = bufferSizeExcludingRouting;
        } else {
            bufferSize = bufferSizeExcludingRouting + "?routing=".length() + encodedRouting.length();
        }

        StringBuilder location = new StringBuilder(bufferSize);
        location.append('/').append(encodedIndex);
        location.append('/').append(encodedType);
        location.append('/').append(encodedId);
        if (encodedRouting != null) {
            location.append("?routing=").append(encodedRouting);
        }

        return location.toString();
    }

    public void writeThin(StreamOutput out) throws IOException {
        super.writeTo(out);
        this.writeWithoutShardId(out);
    }

    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        this.shardId.writeTo(out);
        this.writeWithoutShardId(out);
    }

    private void writeWithoutShardId(StreamOutput out) throws IOException {
        out.writeString(this.type);
        out.writeString(this.id);
        out.writeZLong(this.version);
        if (out.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            out.writeZLong(this.seqNo);
            out.writeVLong(this.primaryTerm);
        }

        out.writeBoolean(this.forcedRefresh);
        this.result.writeTo(out);
    }

    public final XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        this.innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        ReplicationResponse.ShardInfo shardInfo = this.getShardInfo();
        builder.field("_index", this.shardId.getIndexName());
        builder.field("_type", this.type);
        builder.field("_id", this.id).field("_version", this.version).field("result", this.getResult().getLowercase());
        if (this.forcedRefresh) {
            builder.field("forced_refresh", true);
        }

        builder.field("_shards", shardInfo);
        if (this.getSeqNo() >= 0L) {
            builder.field("_seq_no", this.getSeqNo());
            builder.field("_primary_term", this.getPrimaryTerm());
        }

        return builder;
    }

    protected static void parseInnerToXContent(XContentParser parser, Builder context) throws IOException {
        XContentParser.Token token = parser.currentToken();
        XContentParserUtils.ensureExpectedToken(Token.FIELD_NAME, token, parser);
        String currentFieldName = parser.currentName();
        token = parser.nextToken();
        if (token.isValue()) {
            if ("_index".equals(currentFieldName)) {
                context.setShardId(new ShardId(new Index(parser.text(), "_na_"), -1));
            } else if ("_type".equals(currentFieldName)) {
                context.setType(parser.text());
            } else if ("_id".equals(currentFieldName)) {
                context.setId(parser.text());
            } else if ("_version".equals(currentFieldName)) {
                context.setVersion(parser.longValue());
            } else if ("result".equals(currentFieldName)) {
                String result = parser.text();
                Result[] var5 = DocWriteResponse.Result.values();
                int var6 = var5.length;

                for(int var7 = 0; var7 < var6; ++var7) {
                    Result r = var5[var7];
                    if (r.getLowercase().equals(result)) {
                        context.setResult(r);
                        break;
                    }
                }
            } else if ("forced_refresh".equals(currentFieldName)) {
                context.setForcedRefresh(parser.booleanValue());
            } else if ("_seq_no".equals(currentFieldName)) {
                context.setSeqNo(parser.longValue());
            } else if ("_primary_term".equals(currentFieldName)) {
                context.setPrimaryTerm(parser.longValue());
            }
        } else if (token == Token.START_OBJECT) {
            if ("_shards".equals(currentFieldName)) {
                context.setShardInfo(ShardInfo.fromXContent(parser));
            } else {
                parser.skipChildren();
            }
        } else if (token == Token.START_ARRAY) {
            parser.skipChildren();
        }

    }

    public static enum Result implements Writeable {
        CREATED(0),
        UPDATED(1),
        DELETED(2),
        NOT_FOUND(3),
        NOOP(4);

        private final byte op;
        private final String lowercase;

        private Result(int op) {
            this.op = (byte)op;
            this.lowercase = this.name().toLowerCase(Locale.ROOT);
        }

        public byte getOp() {
            return this.op;
        }

        public String getLowercase() {
            return this.lowercase;
        }

        public static Result readFrom(StreamInput in) throws IOException {
            Byte opcode = in.readByte();
            switch (opcode) {
                case 0:
                    return CREATED;
                case 1:
                    return UPDATED;
                case 2:
                    return DELETED;
                case 3:
                    return NOT_FOUND;
                case 4:
                    return NOOP;
                default:
                    throw new IllegalArgumentException("Unknown result code: " + opcode);
            }
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(this.op);
        }
    }

    public abstract static class Builder {
        protected ShardId shardId = null;
        protected String type = null;
        protected String id = null;
        protected Long version = null;
        protected Result result = null;
        protected boolean forcedRefresh;
        protected ReplicationResponse.ShardInfo shardInfo = null;
        protected long seqNo = -2L;
        protected long primaryTerm = 0L;

        public Builder() {
        }

        public ShardId getShardId() {
            return this.shardId;
        }

        public void setShardId(ShardId shardId) {
            this.shardId = shardId;
        }

        public String getType() {
            return this.type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getId() {
            return this.id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setVersion(Long version) {
            this.version = version;
        }

        public void setResult(Result result) {
            this.result = result;
        }

        public void setForcedRefresh(boolean forcedRefresh) {
            this.forcedRefresh = forcedRefresh;
        }

        public void setShardInfo(ReplicationResponse.ShardInfo shardInfo) {
            this.shardInfo = shardInfo;
        }

        public void setSeqNo(long seqNo) {
            this.seqNo = seqNo;
        }

        public void setPrimaryTerm(long primaryTerm) {
            this.primaryTerm = primaryTerm;
        }

        public abstract DocWriteResponse build();
    }
}
