package com.github.dts.util;


import com.google.common.collect.Lists;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * ES 连接器 Rest 两种方式
 *
 * @author rewerma 2019-08-01
 * @version 1.0.0
 */
public class ESConnection {
    public static final ESBulkResponseImpl EMPTY_RESPONSE = new ESBulkResponseImpl(Collections.emptyList());
    private static final Logger logger = LoggerFactory.getLogger(ESConnection.class);
    private final RestHighLevelClient restHighLevelClient;
    private final int concurrentBulkRequest;
    private final int bulkCommitSize;
    private final int maxRetryCount;
    private final int bulkRetryCount;
    private final int minAvailableSpaceHighBulkRequests;
    private final Map<String, CompletableFuture<com.github.dts.util.ESBulkRequest.EsRefreshResponse>> refreshAsyncCache = new ConcurrentHashMap<>(2);
    private final Map<String, CompletableFuture<Map<String, Object>>> getMappingAsyncCache = new ConcurrentHashMap<>(2);
    private final int updateByQueryChunkSize;
    private final String[] elasticsearchUri;
    private long requestEntityTooLargeBytes = 0;

    public ESConnection(CanalConfig.OuterAdapterConfig.EsAccount esAccount) {
        String[] elasticsearchUri = esAccount.getAddress();
        this.elasticsearchUri = elasticsearchUri;
        HttpHost[] httpHosts = Arrays.stream(elasticsearchUri).map(HttpHost::create).toArray(HttpHost[]::new);
        String name = esAccount.getUsername();
        String pwd = esAccount.getPassword();
        String apiKey = esAccount.getApiKey();
        String clusterName = esAccount.getClusterName();
        int concurrentBulkRequest = esAccount.getConcurrentBulkRequest();

        BasicHeader basicHeader;
        if (apiKey != null && !apiKey.isEmpty()) {
            basicHeader = new BasicHeader("Authorization", "ApiKey " + apiKey);
        } else {
            basicHeader = null;
        }

        final RestClientBuilder clientBuilder = RestClient
                .builder(httpHosts)
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(esAccount.getHttpConnectTimeout())
                        .setConnectionRequestTimeout(esAccount.getHttpRequestTimeout())
                        .setSocketTimeout(esAccount.getHttpSocketTimeout()));

        if (basicHeader != null) {
            clientBuilder.setDefaultHeaders(new Header[]{basicHeader});
        }
        clientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
            IOReactorConfig reactorConfig = IOReactorConfig.custom()
                    .setIoThreadCount(Math.max(concurrentBulkRequest, Runtime.getRuntime().availableProcessors()))
                    .setSelectInterval(100)
                    .setSoKeepAlive(true)
                    .build();
            if (basicHeader != null) {
                httpClientBuilder.setDefaultHeaders(Collections.singletonList(basicHeader));
            } else if (name != null && !name.trim().isEmpty()) {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(name, pwd));
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
            httpClientBuilder.setMaxConnTotal(concurrentBulkRequest);
            httpClientBuilder.setMaxConnPerRoute(concurrentBulkRequest);
            httpClientBuilder.setDefaultIOReactorConfig(reactorConfig);
            httpClientBuilder.setKeepAliveStrategy((response, context) -> TimeUnit.MINUTES.toMillis(esAccount.getHttpKeepAliveMinutes()));
            return httpClientBuilder;
        });
        if (clusterName != null && !clusterName.isEmpty()) {
            clientBuilder.setPathPrefix(clusterName);
        }
        this.updateByQueryChunkSize = esAccount.getUpdateByQueryChunkSize();
        this.minAvailableSpaceHighBulkRequests = esAccount.getMinAvailableSpaceHighBulkRequests();
        this.maxRetryCount = esAccount.getMaxRetryCount();
        this.bulkRetryCount = esAccount.getBulkRetryCount();
        this.bulkCommitSize = esAccount.getBulkCommitSize();
        this.concurrentBulkRequest = concurrentBulkRequest;
        this.restHighLevelClient = new RestHighLevelClient(clientBuilder);
    }

    public int getUpdateByQueryChunkSize() {
        return updateByQueryChunkSize;
    }

    public boolean isMaxBatchSize(int size) {
        return size >= bulkCommitSize;
    }

    public void close() {
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            Util.sneakyThrows(e);
        }
    }

    public CompletableFuture<com.github.dts.util.ESBulkRequest.EsRefreshResponse> refreshAsync(String... indices) {
        if (indices.length == 0) {
            return CompletableFuture.completedFuture(null);
        }
        return refreshAsyncCache.computeIfAbsent(String.join(",", indices), key -> {
            CompletableFuture<com.github.dts.util.ESBulkRequest.EsRefreshResponse> future = new CompletableFuture<>();
            RefreshRequest request = new RefreshRequest(indices);
            Cancellable cancellable = restHighLevelClient.indices().refreshAsync(request, RequestOptions.DEFAULT, new ActionListener<RefreshResponse>() {
                @Override
                public void onResponse(RefreshResponse refreshResponse) {
                    refreshAsyncCache.remove(key);
                    future.complete(new Es7RefreshResponse(indices, refreshResponse));
                }

                @Override
                public void onFailure(Exception e) {
                    refreshAsyncCache.remove(key);
                    future.completeExceptionally(e);
                }
            });
            return future;
        });
    }

    public CompletableFuture<Map<String, Object>> getMapping(String index) {
        return getMappingAsyncCache.computeIfAbsent(index, cacheKey -> {
            CompletableFuture<Map<String, Object>> future = new CompletableFuture<>();
            GetMappingsRequest request = new GetMappingsRequest();
            request.setMasterTimeout(null);
            request.indices(index);
            restHighLevelClient.indices()
                    .getMappingAsync(request, RequestOptions.DEFAULT, new ActionListener<GetMappingsResponse>() {
                        @Override
                        public void onResponse(GetMappingsResponse response) {
                            getMappingAsyncCache.remove(cacheKey);
                            Map<String, MappingMetadata> mappings = response.mappings();
                            MappingMetadata mappingMetaData = null;
                            for (String key : mappings.keySet()) {
                                if (key.startsWith(index)) {
                                    mappingMetaData = mappings.get(key);
                                    break;
                                }
                            }
                            if (mappingMetaData == null && !mappings.isEmpty()) {
                                mappingMetaData = mappings.values().iterator().next();
                            }

                            if (mappingMetaData != null) {
                                Map<String, Object> sourceAsMap = mappingMetaData.getSourceAsMap();
                                if (sourceAsMap == null || sourceAsMap.isEmpty()) {
                                    future.completeExceptionally(new IllegalStateException(
                                            String.format("Empty mapping info of index: %s. you can check url, GET /%s/_mapping", index, index)));
                                } else {
                                    future.complete(sourceAsMap);
                                }
                            } else {
                                future.completeExceptionally(new IllegalArgumentException(
                                        String.format("Not found the mapping info of index: %s. you can check url, GET /%s/_mapping", index, index)));
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            getMappingAsyncCache.remove(cacheKey);
                            future.completeExceptionally(e);
                        }
                    });
            return future;
        });
    }

    public String[] getElasticsearchUri() {
        return elasticsearchUri;
    }

    @Override
    public String toString() {
        return "ESConnection{" +
                "concurrentBulkRequest=" + concurrentBulkRequest +
                ", bulkCommitSize=" + bulkCommitSize +
                ", updateByQueryChunkSize=" + updateByQueryChunkSize +
                ", maxRetryCount=" + maxRetryCount +
                ", bulkRetryCount=" + bulkRetryCount +
                '}';
    }

    public static class Es7RefreshResponse implements com.github.dts.util.ESBulkRequest.EsRefreshResponse {
        private final RefreshResponse refreshResponse;
        private final String[] indices;

        public Es7RefreshResponse(String[] indices, RefreshResponse refreshResponse) {
            this.indices = indices;
            this.refreshResponse = refreshResponse;
        }

        @Override
        public String[] getIndices() {
            return indices;
        }

        @Override
        public String toString() {
            return refreshResponse.toString();
        }
    }

    public static class ESBulkResponseImpl implements com.github.dts.util.ESBulkRequest.ESBulkResponse {

        private final List<BulkRequestResponse> bulkResponse;

        public ESBulkResponseImpl(List<BulkRequestResponse> bulkResponse) {
            this.bulkResponse = bulkResponse;
        }

        public ESBulkResponseImpl(com.github.dts.util.ESBulkRequest.ESBulkResponse... responses) {
            this.bulkResponse = Arrays.stream(responses)
                    .filter(Objects::nonNull)
                    .filter(e -> e instanceof ESBulkResponseImpl)
                    .map(e -> (ESBulkResponseImpl) e)
                    .flatMap(e -> e.bulkResponse.stream())
                    .collect(Collectors.toList());
        }

        public static ESBulkResponseImpl merge(com.github.dts.util.ESBulkRequest.ESBulkResponse... responses) {
            return new ESBulkResponseImpl(responses);
        }

        @Override
        public int size() {
            int size = 0;
            for (BulkRequestResponse requestResponse : bulkResponse) {
                size += requestResponse.size();
            }
            return size;
        }

        @Override
        public long requestTotalEstimatedSizeInBytes() {
            long totalEstimatedSizeInBytes = 0L;
            for (BulkRequestResponse response : bulkResponse) {
                totalEstimatedSizeInBytes += response.totalEstimatedSizeInBytes;
            }
            return totalEstimatedSizeInBytes;
        }

        @Override
        public long requestEstimatedSizeInBytes() {
            long estimatedSizeInBytes = 0L;
            for (BulkRequestResponse response : bulkResponse) {
                estimatedSizeInBytes += response.estimatedSizeInBytes;
            }
            return estimatedSizeInBytes;
        }

        @Override
        public String[] requestBytesToString() {
            String[] requestBytes = new String[bulkResponse.size()];
            for (int i = 0, size = bulkResponse.size(); i < size; i++) {
                requestBytes[i] = bulkResponse.get(i).requestBytesToString();
            }
            return requestBytes;
        }

        @Override
        public boolean hasFailures() {
            for (BulkRequestResponse bulkItemResponses : bulkResponse) {
                if (bulkItemResponses.hasFailures()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public List<com.github.dts.util.ESBulkRequest.Failure> getFailureNotFoundList() {
            ArrayList<com.github.dts.util.ESBulkRequest.Failure> failures = new ArrayList<>();
            for (BulkRequestResponse bulkItemResponses : bulkResponse) {
                Collection<BulkItemResponse.Failure> failureNotFoundList = bulkItemResponses.getFailureNotFoundList();
                for (BulkItemResponse.Failure failure : failureNotFoundList) {
                    failures.add(new com.github.dts.util.ESBulkRequest.Failure(
                            failure.getIndex(),
                            failure.getType(),
                            failure.getId(),
                            failure.getCause(),
                            failure.getStatus(),
                            failure.getSeqNo(),
                            failure.getTerm(),
                            failure.isAborted()
                    ));
                }
            }
            failures.trimToSize();
            return failures;
        }

        @Override
        public boolean isEmpty() {
            for (BulkRequestResponse requestResponse : bulkResponse) {
                if (!requestResponse.isEmpty()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void processFailBulkResponse(String errorMsg) throws RuntimeException {
            Set<String> errorRespList = new LinkedHashSet<>();
            for (BulkRequestResponse bulkItemResponses : bulkResponse) {
                errorRespList.addAll(bulkItemResponses.processFailBulkResponse());
            }
            if (!errorRespList.isEmpty()) {
                String msg = String.join(",\n", errorRespList);
                throw new RuntimeException(errorMsg + "[" + msg + "]");
            }
        }
    }

    public static class ESUpdateByQueryRequestImpl extends UpdateByQueryRequest implements com.github.dts.util.ESBulkRequest.ESUpdateByQueryRequest {
        private final Map<String, Object> params = new HashMap<>();
        private int size = 1;
        private int estimatedSizeInBytes;

        public ESUpdateByQueryRequestImpl(String index) {
            super(index);
        }

        public static ESUpdateByQueryRequestImpl byIds(String index, String[] ids, String fieldName, Object fieldValue) {
            ESUpdateByQueryRequestImpl updateByQueryRequest = new ESUpdateByQueryRequestImpl(index);
            updateByQueryRequest.params.put("v", fieldValue);
            updateByQueryRequest.size = ids.length;
            updateByQueryRequest.setQuery(QueryBuilders.idsQuery().addIds(ids));
            updateByQueryRequest.setBatchSize(ids.length);
            updateByQueryRequest.setScript(new Script(ScriptType.INLINE, "painless",
                    "ctx._source." + fieldName + "= params.v", updateByQueryRequest.params));
            updateByQueryRequest.setAbortOnVersionConflict(false);
            return updateByQueryRequest;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void beforeBulk() {
            VectorCompletableFuture.beforeBulk(params);
            this.estimatedSizeInBytes = countEstimatedSizeInBytes();
        }

        @Override
        public int size() {
            return size;
        }

        public ESUpdateByQueryRequestImpl build() {
            return this;
        }

        private int countEstimatedSizeInBytes() {
            int[] bytesCount = new int[1];
            try {
                super.writeTo(new StreamOutput() {
                    @Override
                    public void writeByte(byte b) throws IOException {
                        bytesCount[0] += 1;
                    }

                    @Override
                    public void writeBytes(byte[] b, int offset, int length) throws IOException {
                        bytesCount[0] += length;
                    }

                    @Override
                    public void flush() throws IOException {

                    }

                    @Override
                    public void close() throws IOException {

                    }

                    @Override
                    public void reset() throws IOException {

                    }
                });
            } catch (IOException ignored) {
                // 不会有
            }
            return bytesCount[0];
        }

        public int estimatedSizeInBytes() {
            return estimatedSizeInBytes;
        }
    }

    public static class ESUpdateRequestImpl extends UpdateRequest implements com.github.dts.util.ESBulkRequest.ESUpdateRequest {
        private final String index;
        private final String id;
        private final Map source;
        private final boolean shouldUpsertDoc;

        public ESUpdateRequestImpl(String index, String id, Map source, boolean shouldUpsertDoc, int retryOnConflict) {
            super(index, id);
            docAsUpsert(shouldUpsertDoc);
            retryOnConflict(retryOnConflict);
//            updateRequest.type("");
            this.index = index;
            this.id = id;
            this.source = source;
            this.shouldUpsertDoc = shouldUpsertDoc;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void beforeBulk() {
            Map source = new LinkedHashMap(this.source);
            VectorCompletableFuture.beforeBulk(source);
            doc(source);
        }

        @Override
        public boolean isOverlap(TrimRequest prev) {
            if (prev instanceof ESDeleteRequestImpl) {
                return false;
            } else if (prev instanceof ESUpdateRequestImpl) {
                ESUpdateRequestImpl that = ((ESUpdateRequestImpl) prev);
                return Objects.equals(this.id, that.id) && Objects.equals(this.index, that.index)
                        && this.shouldUpsertDoc == that.shouldUpsertDoc && equalsSourceKey(this.source.keySet(), that.source.keySet());
            } else {
                return false;
            }
        }

        private boolean equalsSourceKey(Set<?> keys1, Set<?> keys2) {
            if (keys1.size() != keys2.size()) {
                return false;
            }
            for (Object key1 : keys1) {
                if (!keys2.contains(key1)) {
                    return false;
                }
            }
            return true;
        }

        public String getIndex() {
            return index;
        }

        public String getId() {
            return id;
        }

        public ESUpdateRequestImpl build() {
            return this;
        }
    }

    public static class ESDeleteRequestImpl extends DeleteRequest implements com.github.dts.util.ESBulkRequest.ESDeleteRequest {

        private final String index;
        private final String id;

        public ESDeleteRequestImpl(String index, String id) {
            super(index, id);
            this.index = index;
            this.id = id;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean isOverlap(TrimRequest prev) {
            if (prev instanceof ESDeleteRequestImpl) {
                ESDeleteRequestImpl that = ((ESDeleteRequestImpl) prev);
                return Objects.equals(this.index, that.index)
                        && Objects.equals(this.id, that.id);
            } else if (prev instanceof ESUpdateRequestImpl) {
                ESUpdateRequestImpl that = ((ESUpdateRequestImpl) prev);
                return Objects.equals(this.index, that.index)
                        && Objects.equals(this.id, that.id);
            } else {
                return false;
            }
        }

        public ESDeleteRequestImpl build() {
            return this;
        }
    }

    public static class ConcurrentBulkRequest extends BulkRequest {
        private final int id;
        private final OwnerReentrantLock lock = new OwnerReentrantLock();
        private final List<ESUpdateByQueryRequestImpl> bbUpdateByQueryRequests;
        private final List<DocWriteRequest<?>> bbDocWriteRequests;
        private long estimatedSizeInBytes;
        private long updateByQueryRequestsEstimatedSizeInBytes;

        static class OwnerReentrantLock extends ReentrantLock {
            @Override
            public Thread getOwner() {
                return super.getOwner();
            }
        }

        public ConcurrentBulkRequest(int id, int bulkCommitSize) {
            this.id = id;
            this.bbUpdateByQueryRequests = new ArrayList<>(bulkCommitSize);
            this.bbDocWriteRequests = new ArrayList<>(bulkCommitSize);
        }

        public int getId() {
            return id;
        }

        public boolean isHeldByCurrentThread() {
            return lock.isHeldByCurrentThread();
        }

        public boolean tryLock() {
            return lock.tryLock();
        }

        public void unlock() {
            lock.unlock();
        }

        public void clear() {
            super.requests().clear();
        }

        @Override
        public BulkRequest add(DocWriteRequest<?> request) {
            return super.add(request);
        }

        public <T extends com.github.dts.util.ESBulkRequest.ESRequest & DocWriteRequest<?>> void addBulkBuffer(T request) {
            bbDocWriteRequests.add(request);
        }

        public void addBulkBuffer(ESUpdateByQueryRequestImpl request) {
            bbUpdateByQueryRequests.add(request);
        }

        public void beforeBulk() {
            // 调用这里前，有tryLock
//            assert lock.isLocked();
            long beforeEstimatedSizeInBytes = totalEstimatedSizeInBytes();
            try {
                for (DocWriteRequest<?> request : bbDocWriteRequests) {
                    if (request instanceof com.github.dts.util.ESBulkRequest.ESRequest) {
                        ((com.github.dts.util.ESBulkRequest.ESRequest) request).beforeBulk();
                    }
                    super.add(request);
                }
                bbDocWriteRequests.clear();

                long updateByQueryRequestsEstimatedSizeInBytes = 0;
                for (ESUpdateByQueryRequestImpl request : bbUpdateByQueryRequests) {
                    request.beforeBulk();
                    updateByQueryRequestsEstimatedSizeInBytes += request.estimatedSizeInBytes();
                }
                this.updateByQueryRequestsEstimatedSizeInBytes += updateByQueryRequestsEstimatedSizeInBytes;
            } finally {
                this.estimatedSizeInBytes = totalEstimatedSizeInBytes() - beforeEstimatedSizeInBytes;
            }
        }

        public int requestNumberOfActions() {
            return super.numberOfActions() + bbUpdateByQueryRequests.size() + bbDocWriteRequests.size();
        }

        public ESUpdateByQueryRequestImpl pollUpdateByQuery() {
            int size = bbUpdateByQueryRequests.size();
            return size == 0 ? null : bbUpdateByQueryRequests.remove(size - 1);
        }

        @Override
        public String toString() {
            Thread owner = lock.getOwner();
            double kb = Math.round((double) estimatedSizeInBytes * 100D / 1024D) / 100D;
            String kbString = (kb >= 1D || kb == 0D ? String.valueOf(Math.round(kb)) : String.valueOf(kb));
            return "ConcurrentBulkRequest{" +
                    "id=" + id +
                    ", size=" + requestNumberOfActions() +
                    ", historyBytes=" + kbString + "/kb" +
                    ", lock=" + (owner == null ? "[Unlocked]" : "[Locked by thread " + owner.getName() + "]") +
                    '}';
        }

        public long totalEstimatedSizeInBytes() {
            return super.estimatedSizeInBytes() + this.updateByQueryRequestsEstimatedSizeInBytes;
        }

        @Override
        public long estimatedSizeInBytes() {
            return estimatedSizeInBytes;
        }
    }

    public static class BulkRequestResponse {
        private final ConcurrentBulkRequest request;
        private final long estimatedSizeInBytes;
        private final long totalEstimatedSizeInBytes;
        private final List<UpdateByQuery> updateByQueryList = new ArrayList<>();
        private BulkResponse response;

        public BulkRequestResponse(ConcurrentBulkRequest request) {
            this.request = request;
            this.estimatedSizeInBytes = request.estimatedSizeInBytes();
            this.totalEstimatedSizeInBytes = request.totalEstimatedSizeInBytes();
        }

        private static String toError(BulkItemResponse e) {
            return "[" + e.getOpType() + "]. " + e.getFailure().getCause().getClass() + ": " + e.getFailure();
        }

        public List<BulkItemResponse.Failure> getFailureNotFoundList() {
            ArrayList<BulkItemResponse.Failure> errorRespList = new ArrayList<>();
            for (BulkItemResponse itemResponse : response.getItems()) {
                if (!itemResponse.isFailed()) {
                    continue;
                }
                BulkItemResponse.Failure failure = itemResponse.getFailure();
                if (failure.getStatus() == RestStatus.NOT_FOUND) {
                    errorRespList.add(failure);
                }
            }
            return errorRespList;
        }

        public List<String> processFailBulkResponse() throws RuntimeException {
            int notfound = 0;
            List<String> errorRespList = new ArrayList<>();
            for (BulkItemResponse itemResponse : response.getItems()) {
                if (!itemResponse.isFailed()) {
                    continue;
                }
                RestStatus status = itemResponse.getFailure().getStatus();
                if (status == RestStatus.NOT_FOUND) {
                    if (notfound++ == 0) {
                        logger.warn("notfound {}", itemResponse.getFailureMessage());
                    }
                } else if (status == RestStatus.CONFLICT) {
                    logger.warn("conflict {}", itemResponse.getFailureMessage());
                } else if (status == RestStatus.TOO_MANY_REQUESTS) {
                    throw new RuntimeException(toError(itemResponse));
                } else {
                    errorRespList.add(toError(itemResponse));
                }
            }
            for (UpdateByQuery updateByQuery : updateByQueryList) {
                List<BulkItemResponse.Failure> failures = updateByQuery.response.getBulkFailures();
                for (BulkItemResponse.Failure failure : failures) {
                    logger.warn("updateByQueryFail {}", failure.toString());
                }
            }
            return errorRespList;
        }

        public void addUpdateByQueryResponse(ESUpdateByQueryRequestImpl request, BulkByScrollResponse response) {
            updateByQueryList.add(new UpdateByQuery(request, response));
        }

        public String requestBytesToString() {
            double kb = Math.round((double) estimatedSizeInBytes * 100D / 1024D) / 100D;
            double mb = Math.round((double) totalEstimatedSizeInBytes * 100D / 1024D / 1024D) / 100D;
            return request.getId() + ":" + (kb >= 1D || kb == 0D ? String.valueOf(Math.round(kb)) : kb) + "kb/" + (mb >= 1D || mb == 0D ? String.valueOf(Math.round(mb)) : mb) + "mb";
        }

        public boolean hasFailures() {
            if (response.hasFailures()) {
                return true;
            }
            for (UpdateByQuery updateByQuery : updateByQueryList) {
                List<BulkItemResponse.Failure> failures = updateByQuery.response.getBulkFailures();
                if (!failures.isEmpty()) {
                    return true;
                }
            }
            return false;
        }

        public boolean isEmpty() {
            return response.getItems().length == 0 && updateByQueryList.isEmpty();
        }

        public int size() {
            int size = response.getItems().length;
            for (UpdateByQuery item : updateByQueryList) {
                size += item.request.size();
            }
            return size;
        }

        private static class UpdateByQuery {
            ESUpdateByQueryRequestImpl request;
            BulkByScrollResponse response;

            private UpdateByQuery(ESUpdateByQueryRequestImpl request, BulkByScrollResponse response) {
                this.request = request;
                this.response = response;
            }
        }
    }

    public static class ESSearchRequest {
        private final SearchRequest searchRequest;

        public ESSearchRequest(String index) {
            searchRequest = new SearchRequest(index);
        }

        public ESSearchRequest searchAfter(Object[] values) {
            searchRequest.source().searchAfter(values);
            return this;
        }

        public ESSearchRequest setQuery(QueryBuilder queryBuilder) {
            searchRequest.source().query(queryBuilder);
            return this;
        }

        public ESSearchRequest size(int size) {
            searchRequest.source().size(size);
            return this;
        }

        public ESSearchRequest sort(String name, String asc) {
            searchRequest.source().sort(name, SortOrder.fromString(asc));
            return this;
        }

        public ESSearchRequest fetchSource(String[] includes, String[] excludes) {
            searchRequest.source().fetchSource(includes, excludes);
            return this;
        }

        public ESSearchRequest fetchSource(String... includes) {
            searchRequest.source().fetchSource(includes, null);
            return this;
        }

        public SearchResponse getResponse(ESConnection esConnection) {
            try {
                return esConnection.restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            } catch (Exception e) {
                Util.sneakyThrows(e);
                return null;
            }
        }
    }

    public static class ESBulkRequest implements com.github.dts.util.ESBulkRequest {
        private static final int BYTES_1_MB = 1024 * 1024;
        private final ConcurrentBulkRequest[] bulkRequests;
        // high list = [0,1,2,3,4,5,6,7]
        private final List<ConcurrentBulkRequest> highBulkRequests;
        // low list = [7,6,5,4,3,2]
        private final List<ConcurrentBulkRequest> lowBulkRequests;
        private final ESConnection connection;
        private final int bulkCommitSize;
        private final int maxRetryCount;
        private final Map<DocWriteRequest<?>, Integer> retryCounter = new ConcurrentHashMap<>();

        public ESBulkRequest(ESConnection connection) {
            this.connection = connection;
            this.maxRetryCount = connection.maxRetryCount;
            this.bulkCommitSize = Math.max(1, connection.bulkCommitSize);
            bulkRequests = new ConcurrentBulkRequest[Math.max(1, connection.concurrentBulkRequest)];
            for (int i = 0; i < bulkRequests.length; i++) {
                bulkRequests[i] = new ConcurrentBulkRequest(i, bulkCommitSize);
            }
            this.highBulkRequests = Arrays.asList(bulkRequests);
            ArrayList<ConcurrentBulkRequest> lowBulkRequests = new ArrayList<>(Arrays.asList(bulkRequests));
            if (lowBulkRequests.size() >= connection.minAvailableSpaceHighBulkRequests + 1) {
                if (connection.minAvailableSpaceHighBulkRequests > 0) {
                    lowBulkRequests.subList(0, connection.minAvailableSpaceHighBulkRequests).clear();
                }
            }
            Collections.reverse(lowBulkRequests);
            this.lowBulkRequests = lowBulkRequests;
        }

        private static void yieldThreadRandom() {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(5, 100));
            } catch (InterruptedException e) {
                Util.sneakyThrows(e);
            }
        }

        private static void yieldThread() {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Util.sneakyThrows(e);
            }
        }

        private static boolean hasTooManyRequests(BulkResponse responses) {
            for (BulkItemResponse response : responses.getItems()) {
                if (response.status() == RestStatus.TOO_MANY_REQUESTS) {
                    return true;
                }
            }
            return false;
        }

        private static BulkRequest[] partition(BulkRequest request, int size) {
            List<DocWriteRequest<?>> requests = request.requests();
            List<List<DocWriteRequest<?>>> partition = Lists.partition(requests, size);
            BulkRequest[] result = new BulkRequest[partition.size()];
            Iterator<List<DocWriteRequest<?>>> iterator = partition.iterator();
            for (int i = 0; i < result.length; i++) {
                BulkRequest item = new BulkRequest();
                iterator.next().forEach(item::add);//retry
                result[i] = item;
            }
            return result;
        }

        private Collection<ConcurrentBulkRequest> prioritySort(BulkPriorityEnum priorityEnum) {
            switch (priorityEnum) {
                case RANDOM: {
                    ArrayList<ConcurrentBulkRequest> shuffle = new ArrayList<>(Arrays.asList(bulkRequests));
                    Collections.shuffle(shuffle);
                    return shuffle;
                }
                case HIGH: {
                    return highBulkRequests;
                }
                default:
                case LOW: {
                    return lowBulkRequests;
                }
            }
        }

        @Override
        public com.github.dts.util.ESBulkRequest add(Collection<ESRequest> requests, BulkPriorityEnum priorityEnum) {
            if (requests.isEmpty()) {
                return this;
            }
            Collection<ConcurrentBulkRequest> concurrentBulkRequests = prioritySort(priorityEnum);
            while (true) {
                for (ConcurrentBulkRequest bulkRequest : concurrentBulkRequests) {
                    if (bulkRequest.requestNumberOfActions() < bulkCommitSize && bulkRequest.tryLock()) {
                        try {
                            for (Object request : requests) {
                                if (request instanceof ESUpdateByQueryRequestImpl) {
                                    bulkRequest.addBulkBuffer(((ESUpdateByQueryRequestImpl) request).build());
                                } else if (request instanceof ESUpdateRequestImpl) {
                                    bulkRequest.addBulkBuffer(((ESUpdateRequestImpl) request).build());
                                } else if (request instanceof ESDeleteRequestImpl) {
                                    bulkRequest.addBulkBuffer(((ESDeleteRequestImpl) request).build());
                                } else {
                                    throw new IllegalArgumentException("Unknown request type: " + request.getClass());
                                }
                            }
                        } finally {
                            bulkRequest.unlock();
                        }
                        return this;
                    }
                }
                yieldThread();
            }
        }

        @Override
        public com.github.dts.util.ESBulkRequest add(ESUpdateByQueryRequest esUpdateRequest) {
            ESUpdateByQueryRequestImpl eir = (ESUpdateByQueryRequestImpl) esUpdateRequest;
            while (true) {
                for (ConcurrentBulkRequest bulkRequest : bulkRequests) {
                    if (bulkRequest.requestNumberOfActions() < bulkCommitSize && bulkRequest.tryLock()) {
                        try {
                            bulkRequest.addBulkBuffer(eir.build());
                        } finally {
                            bulkRequest.unlock();
                        }
                        return this;
                    }
                }
                yieldThread();
            }
        }

        @Override
        public ESBulkRequest add(ESUpdateRequest esUpdateRequest) {
            ESUpdateRequestImpl eur = (ESUpdateRequestImpl) esUpdateRequest;
            while (true) {
                for (ConcurrentBulkRequest bulkRequest : bulkRequests) {
                    if (bulkRequest.requestNumberOfActions() < bulkCommitSize && bulkRequest.tryLock()) {
                        try {
                            bulkRequest.addBulkBuffer(eur.build());
                        } finally {
                            bulkRequest.unlock();
                        }
                        return this;
                    }
                }
                yieldThread();
            }
        }

        @Override
        public ESBulkRequest add(ESDeleteRequest esDeleteRequest) {
            ESDeleteRequestImpl edr = (ESDeleteRequestImpl) esDeleteRequest;
            while (true) {
                for (ConcurrentBulkRequest bulkRequest : bulkRequests) {
                    if (bulkRequest.requestNumberOfActions() < bulkCommitSize && bulkRequest.tryLock()) {
                        try {
                            bulkRequest.addBulkBuffer(edr.build());
                        } finally {
                            bulkRequest.unlock();
                        }
                        return this;
                    }
                }
                yieldThread();
            }
        }

        @Override
        public int numberOfActions() {
            int count = 0;
            for (ConcurrentBulkRequest bulkRequest : bulkRequests) {
                count += bulkRequest.requestNumberOfActions();
            }
            return count;
        }

        @Override
        public boolean isEmpty() {
            for (ConcurrentBulkRequest bulkRequest : bulkRequests) {
                if (bulkRequest.requestNumberOfActions() > 0) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public ESBulkResponse bulk() {
            List<BulkRequestResponse> bulkResponse = new ArrayList<>(bulkRequests.length);
            ESBulkResponseImpl esBulkResponseImpl = new ESBulkResponseImpl(bulkResponse);
            for (ConcurrentBulkRequest bulkRequest : bulkRequests) {
                if (bulkRequest.requestNumberOfActions() > 0 && bulkRequest.tryLock()) {
                    try {
                        BulkRequestResponse requestResponse = commit(bulkRequest, maxRetryCount > 0);
                        bulkResponse.add(requestResponse);
                    } catch (Exception e) {
                        Util.sneakyThrows(e);
                        return null;
                    } finally {
                        bulkRequest.unlock();
                    }
                }
            }
            return esBulkResponseImpl;
        }

        private BulkResponse bulk(BulkRequest bulkRequest) throws IOException {
            if (bulkRequest.numberOfActions() == 0) {
                return new BulkResponse(new BulkItemResponse[0], 0L);
            }
            if (connection.requestEntityTooLargeBytes > 0 && bulkRequest.estimatedSizeInBytes() > connection.requestEntityTooLargeBytes) {
                return requestEntityTooLarge(bulkRequest);
            }
            IOException ioException = null;
            int bulkRetryCount = Math.max(0, connection.bulkRetryCount);
            for (int i = 0; i <= bulkRetryCount; i++) {
                try {
                    return connection.restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                } catch (ElasticsearchStatusException e) {
                    if (e.status() == RestStatus.REQUEST_ENTITY_TOO_LARGE) {
                        long retlBytes = connection.requestEntityTooLargeBytes;
                        long estimatedSizeInBytes = bulkRequest.estimatedSizeInBytes();
                        connection.requestEntityTooLargeBytes = retlBytes > 0 ? Math.min(retlBytes, estimatedSizeInBytes) : estimatedSizeInBytes;
                        return requestEntityTooLarge(bulkRequest);
                    } else {
                        throw e;
                    }
                } catch (IOException e) {
                    ioException = e;
                    yieldThread();
                }
            }
            throw ioException;
        }

        private BulkResponse requestEntityTooLarge(BulkRequest bulkRequest) throws IOException {
            int partitionSize = (int) (bulkRequest.estimatedSizeInBytes() / BYTES_1_MB);
            BulkRequest[] partition = partition(bulkRequest, Math.max(partitionSize, 2));
            BulkResponse response0 = null;
            for (BulkRequest request : partition) {
                BulkResponse response = bulk(request);
                if (response.hasFailures()) {
                    return response;
                }
                if (response0 == null) {
                    response0 = response;
                }
            }
            return response0;
        }

        private BulkRequestResponse commit(ConcurrentBulkRequest bulkRequest, boolean retry) throws IOException {
            bulkRequest.beforeBulk();
            BulkRequestResponse requestResponse = new BulkRequestResponse(bulkRequest);
            BulkResponse response = requestResponse.response = bulk(bulkRequest);
            if (retry && response.hasFailures()) {
                if (hasTooManyRequests(response)) {
                    yieldThreadRandom();
                } else if (bulkRequest.numberOfActions() > 1) {
                    ArrayList<DocWriteRequest<?>> errorRequests1 = new ArrayList<>();
                    ArrayList<DocWriteRequest<?>> errorRequests2 = new ArrayList<>();
                    if (retryAndGetErrorRequests(Collections.unmodifiableList(bulkRequest.requests()), errorRequests1)) {
                        if (retryAndGetErrorRequests(Collections.unmodifiableList(errorRequests1), errorRequests2)) {
                            bulkRequest.clear();
                            for (DocWriteRequest<?> errorRequest : errorRequests2) {
                                bulkRequest.add(errorRequest);//retry
                            }
                        } else {
                            bulkRequest.clear();
                            for (DocWriteRequest<?> errorRequest : errorRequests1) {
                                bulkRequest.add(errorRequest);//retry
                            }
                            yieldThreadRandom();
                        }
                    } else {
                        yieldThreadRandom();
                    }
                }
            } else {
                bulkRequest.clear();
            }

            while (true) {
                ESUpdateByQueryRequestImpl updateByQueryRequest = bulkRequest.pollUpdateByQuery();
                if (updateByQueryRequest == null) {
                    break;
                }
                BulkByScrollResponse updatedByQuery = connection.restHighLevelClient.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);
                requestResponse.addUpdateByQueryResponse(updateByQueryRequest, updatedByQuery);
            }
            return requestResponse;
        }

        private boolean retryAndGetErrorRequests(List<DocWriteRequest<?>> readonlyRequests, List<DocWriteRequest<?>> errorRequests) throws IOException {
            if (readonlyRequests.isEmpty()) {
                return true;
            }
            List<List<DocWriteRequest<?>>> partition = Lists.partition(readonlyRequests, (readonlyRequests.size() + 1) / 2);
            for (List<DocWriteRequest<?>> rowList : partition) {
                BulkRequest bulkRequest = new BulkRequest();
                rowList.forEach(bulkRequest::add);//retry
                BulkResponse bulkItemResponses = bulk(bulkRequest);
                if (hasTooManyRequests(bulkItemResponses)) {
                    return false;
                }
                if (bulkItemResponses.hasFailures()) {
                    if (rowList.size() == 1) {
                        DocWriteRequest<?> retry = readonlyRequests.get(0);
                        int count = retryCounter.computeIfAbsent(retry, e -> 1);
                        if (count < maxRetryCount) {
                            retryCounter.put(retry, count + 1);
                            errorRequests.add(retry);
                        } else {
                            retryCounter.remove(retry);
                        }
                    } else if (!rowList.isEmpty() && !retryAndGetErrorRequests(rowList, errorRequests)) {
                        return false;
                    }
                }
            }
            return true;
        }

        @Override
        public String toString() {
            return "BulkRequest{" +
                    "size=" + numberOfActions() +
                    ", requests=" + Arrays.toString(bulkRequests) +
                    '}';
        }
    }
}
