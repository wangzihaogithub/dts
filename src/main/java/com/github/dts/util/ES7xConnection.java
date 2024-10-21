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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
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
public class ES7xConnection {
    public static final ES7xBulkResponse EMPTY_RESPONSE = new ES7xBulkResponse(Collections.emptyList());
    private static final Logger logger = LoggerFactory.getLogger(ES7xConnection.class);
    private final RestHighLevelClient restHighLevelClient;
    private final int concurrentBulkRequest;
    private final int bulkCommitSize;
    private final int maxRetryCount;
    private final int bulkRetryCount;
    private final int minAvailableSpaceHighBulkRequests;
    private final Map<String, CompletableFuture<ESBulkRequest.EsRefreshResponse>> refreshAsyncCache = new ConcurrentHashMap<>(2);
    private final Map<String, CompletableFuture<Map<String, Object>>> getMappingAsyncCache = new ConcurrentHashMap<>(2);
    private final int updateByQueryChunkSize;

    public ES7xConnection(CanalConfig.OuterAdapterConfig.EsAccount es7x) {
        String[] elasticsearchUri = es7x.getAddress();
        HttpHost[] httpHosts = Arrays.stream(elasticsearchUri).map(HttpHost::create).toArray(HttpHost[]::new);
        String name = es7x.getUsername();
        String pwd = es7x.getPassword();
        String apiKey = es7x.getApiKey();
        String clusterName = es7x.getClusterName();
        int concurrentBulkRequest = es7x.getConcurrentBulkRequest();

        BasicHeader basicHeader;
        if (apiKey != null && !apiKey.isEmpty()) {
            basicHeader = new BasicHeader("Authorization", "ApiKey " + apiKey);
        } else {
            basicHeader = null;
        }

        final RestClientBuilder clientBuilder = RestClient
                .builder(httpHosts)
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(es7x.getHttpConnectTimeout())
                        .setConnectionRequestTimeout(es7x.getHttpRequestTimeout())
                        .setSocketTimeout(es7x.getHttpSocketTimeout()));

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
            } else {
                CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(name, pwd));
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
            httpClientBuilder.setMaxConnTotal(concurrentBulkRequest);
            httpClientBuilder.setMaxConnPerRoute(concurrentBulkRequest);
            httpClientBuilder.setDefaultIOReactorConfig(reactorConfig);
            httpClientBuilder.setKeepAliveStrategy((response, context) -> TimeUnit.MINUTES.toMillis(es7x.getHttpKeepAliveMinutes()));
            return httpClientBuilder;
        });
        if (clusterName != null && !clusterName.isEmpty()) {
            clientBuilder.setPathPrefix(clusterName);
        }
        this.updateByQueryChunkSize = es7x.getUpdateByQueryChunkSize();
        this.minAvailableSpaceHighBulkRequests = es7x.getMinAvailableSpaceHighBulkRequests();
        this.maxRetryCount = es7x.getMaxRetryCount();
        this.bulkRetryCount = es7x.getBulkRetryCount();
        this.bulkCommitSize = es7x.getBulkCommitSize();
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

    public CompletableFuture<ESBulkRequest.EsRefreshResponse> refreshAsync(String... indices) {
        if (indices.length == 0) {
            return CompletableFuture.completedFuture(null);
        }
        return refreshAsyncCache.computeIfAbsent(String.join(",", indices), key -> {
            CompletableFuture<ESBulkRequest.EsRefreshResponse> future = new CompletableFuture<>();
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
                                future.complete(mappingMetaData.getSourceAsMap());
                            } else {
                                future.completeExceptionally(new IllegalArgumentException("Not found the mapping info of index: " + index));
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

    @Override
    public String toString() {
        return "ES7xConnection{" +
                "concurrentBulkRequest=" + concurrentBulkRequest +
                ", bulkCommitSize=" + bulkCommitSize +
                ", updateByQueryChunkSize=" + updateByQueryChunkSize +
                ", maxRetryCount=" + maxRetryCount +
                ", bulkRetryCount=" + bulkRetryCount +
                '}';
    }

    public static class Es7RefreshResponse implements ESBulkRequest.EsRefreshResponse {
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

    public static class ES7xBulkResponse implements ESBulkRequest.ESBulkResponse {

        private final List<BulkRequestResponse> bulkResponse;

        public ES7xBulkResponse(List<BulkRequestResponse> bulkResponse) {
            this.bulkResponse = bulkResponse;
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

    public static class ES7xUpdateByQueryRequest implements ESBulkRequest.ESUpdateByQueryRequest {

        private final UpdateByQueryRequest updateByQueryRequest;
        private int size = 1;

        public ES7xUpdateByQueryRequest(String index) {
            updateByQueryRequest = new UpdateByQueryRequest(index) {
                @Override
                public ActionRequestValidationException validate() {
                    return null;
                }
            };
        }

        public static ES7xUpdateByQueryRequest byIds(String index, String[] ids, String fieldName, Object fieldValue) {
            ES7xUpdateByQueryRequest updateByQueryRequest = new ES7xUpdateByQueryRequest(index);
            updateByQueryRequest.size = ids.length;
            updateByQueryRequest.updateByQueryRequest.setQuery(QueryBuilders.idsQuery().addIds(ids));
            updateByQueryRequest.updateByQueryRequest.setBatchSize(ids.length);
            updateByQueryRequest.updateByQueryRequest.setScript(new Script(ScriptType.INLINE, "painless",
                    "ctx._source." + fieldName + "= params.v", Collections.singletonMap("v", fieldValue)));
            updateByQueryRequest.updateByQueryRequest.setAbortOnVersionConflict(false);
            return updateByQueryRequest;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public String toString() {
            return updateByQueryRequest.toString();
        }

        public ES7xUpdateByQueryRequest build() {
            return this;
        }
    }

    public static class ES7xIndexRequest implements ESBulkRequest.ESIndexRequest {

        private final IndexRequest indexRequest;

        public ES7xIndexRequest(String index, String id, Map<String, ?> source) {
            indexRequest = new IndexRequest(index) {
                @Override
                public ActionRequestValidationException validate() {
                    return null;
                }
            };
            indexRequest.id(id);
            indexRequest.source(source);
//            indexRequest.type("");
        }

        @Override
        public String toString() {
            return indexRequest.toString();
        }

        public IndexRequest build() {
            return indexRequest;
        }

    }

    public static class ES7xUpdateRequest implements ESBulkRequest.ESUpdateRequest {

        private final UpdateRequest updateRequest;
        private final String index;
        private final String id;
        private final Map source;
        private final boolean shouldUpsertDoc;

        public ES7xUpdateRequest(String index, String id, Map source, boolean shouldUpsertDoc, int retryOnConflict) {
            updateRequest = new UpdateRequest(index, id) {
                @Override
                public ActionRequestValidationException validate() {
                    return null;
                }
            };
            updateRequest.docAsUpsert(shouldUpsertDoc);
            updateRequest.doc(source);
            updateRequest.retryOnConflict(retryOnConflict);
//            updateRequest.type("");
            this.index = index;
            this.id = id;
            this.source = source;
            this.shouldUpsertDoc = shouldUpsertDoc;
        }

        @Override
        public boolean isOverlap(TrimRequest prev) {
            if (prev instanceof ES7xDeleteRequest) {
                return false;
            } else if (prev instanceof ES7xUpdateRequest) {
                ES7xUpdateRequest that = ((ES7xUpdateRequest) prev);
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

        @Override
        public String toString() {
            return updateRequest.toString();
        }

        public UpdateRequest build() {
            return updateRequest;
        }
    }

    public static class ES7xDeleteRequest implements ESBulkRequest.ESDeleteRequest {

        private final DeleteRequest deleteRequest;
        private final String index;
        private final String id;

        public ES7xDeleteRequest(String index, String id) {
            deleteRequest = new DeleteRequest(index, id) {
                @Override
                public ActionRequestValidationException validate() {
                    return null;
                }
            };
//            deleteRequest.type("");
            this.index = index;
            this.id = id;
        }

        @Override
        public boolean isOverlap(TrimRequest prev) {
            if (prev instanceof ES7xDeleteRequest) {
                ES7xDeleteRequest that = ((ES7xDeleteRequest) prev);
                return Objects.equals(this.index, that.index)
                        && Objects.equals(this.id, that.id);
            } else if (prev instanceof ES7xUpdateRequest) {
                ES7xUpdateRequest that = ((ES7xUpdateRequest) prev);
                return Objects.equals(this.index, that.index)
                        && Objects.equals(this.id, that.id);
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return deleteRequest.toString();
        }

        public DeleteRequest build() {
            return deleteRequest;
        }
    }

    public static class ConcurrentBulkRequest extends BulkRequest {
        private final ReentrantLock lock = new ReentrantLock();
        private final int id;
        private final List<ES7xUpdateByQueryRequest> updateByQueryRequests = new ArrayList<>();
        private long beforeEstimatedSizeInBytes;

        public ConcurrentBulkRequest(int id) {
            this.id = id;
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
            requests().clear();
            this.beforeEstimatedSizeInBytes = super.estimatedSizeInBytes();
        }

        public void add(ES7xUpdateByQueryRequest updateByQueryRequest) {
            updateByQueryRequests.add(updateByQueryRequest);
        }

        public int requestNumberOfActions() {
            return super.numberOfActions() + updateByQueryRequests.size();
        }

        public ES7xUpdateByQueryRequest pollUpdateByQuery() {
            int size = updateByQueryRequests.size();
            return size == 0 ? null : updateByQueryRequests.remove(size - 1);
        }

        @Override
        public String toString() {
            return "ConcurrentBulkRequest{" +
                    "id=" + id +
                    ", size=" + requestNumberOfActions() +
                    ", lock=" + lock +
                    '}';
        }

        public long totalEstimatedSizeInBytes() {
            return super.estimatedSizeInBytes();
        }

        @Override
        public long estimatedSizeInBytes() {
            return super.estimatedSizeInBytes() - beforeEstimatedSizeInBytes;
        }
    }

    public static class BulkRequestResponse {
        private final ConcurrentBulkRequest request;
        private final List<DocWriteRequest<?>> requests;
        private final long estimatedSizeInBytes;
        private final long totalEstimatedSizeInBytes;
        private final List<UpdateByQuery> updateByQueryList = new ArrayList<>();
        private BulkResponse response;

        public BulkRequestResponse(ConcurrentBulkRequest request) {
            this.request = request;
            this.requests = request.requests();
            this.estimatedSizeInBytes = request.estimatedSizeInBytes();
            this.totalEstimatedSizeInBytes = request.totalEstimatedSizeInBytes();
        }

        private static String toError(BulkItemResponse e) {
            return "[" + e.getOpType() + "]. " + e.getFailure().getCause().getClass() + ": " + e.getFailure();
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

        public void addUpdateByQueryResponse(ES7xUpdateByQueryRequest request, BulkByScrollResponse response) {
            updateByQueryList.add(new UpdateByQuery(request, response));
        }

        public String requestBytesToString() {
            long kb = Math.round((double) estimatedSizeInBytes / 1024);
            long mb = Math.round((double) totalEstimatedSizeInBytes / 1024 / 1024);
            return request.getId() + ":" + kb + "kb/" + mb + "mb";
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
            ES7xUpdateByQueryRequest request;
            BulkByScrollResponse response;

            private UpdateByQuery(ES7xUpdateByQueryRequest request, BulkByScrollResponse response) {
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

        public SearchResponse getResponse(ES7xConnection es7xConnection) {
            try {
                return es7xConnection.restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            } catch (Exception e) {
                Util.sneakyThrows(e);
                return null;
            }
        }
    }

    public static class ES7xBulkRequest implements ESBulkRequest {
        private final ConcurrentBulkRequest[] bulkRequests;
        // high list = [0,1,2,3,4,5,6,7]
        private final List<ConcurrentBulkRequest> highBulkRequests;
        // low list = [7,6,5,4,3,2]
        private final List<ConcurrentBulkRequest> lowBulkRequests;
        private final ES7xConnection connection;
        private final int bulkCommitSize;
        private final int maxRetryCount;
        private final Map<DocWriteRequest<?>, Integer> retryCounter = new ConcurrentHashMap<>();

        public ES7xBulkRequest(ES7xConnection connection) {
            this.connection = connection;
            this.maxRetryCount = connection.maxRetryCount;
            this.bulkCommitSize = Math.max(1, connection.bulkCommitSize);
            bulkRequests = new ConcurrentBulkRequest[Math.max(1, connection.concurrentBulkRequest)];
            for (int i = 0; i < bulkRequests.length; i++) {
                bulkRequests[i] = new ConcurrentBulkRequest(i);
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
        public ESBulkRequest add(Collection<ESRequest> requests, BulkPriorityEnum priorityEnum) {
            if (requests.isEmpty()) {
                return this;
            }
            Collection<ConcurrentBulkRequest> concurrentBulkRequests = prioritySort(priorityEnum);
            while (true) {
                for (ConcurrentBulkRequest bulkRequest : concurrentBulkRequests) {
                    if (bulkRequest.requestNumberOfActions() < bulkCommitSize && bulkRequest.tryLock()) {
                        try {
                            for (Object request : requests) {
                                if (request instanceof ES7xIndexRequest) {
                                    bulkRequest.add(((ES7xIndexRequest) request).build());
                                } else if (request instanceof ES7xUpdateByQueryRequest) {
                                    bulkRequest.add(((ES7xUpdateByQueryRequest) request).build());
                                } else if (request instanceof ES7xUpdateRequest) {
                                    bulkRequest.add(((ES7xUpdateRequest) request).build());
                                } else if (request instanceof ES7xDeleteRequest) {
                                    bulkRequest.add(((ES7xDeleteRequest) request).build());
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
        public ES7xBulkRequest add(ESIndexRequest esIndexRequest) {
            ES7xIndexRequest eir = (ES7xIndexRequest) esIndexRequest;
            while (true) {
                for (ConcurrentBulkRequest bulkRequest : bulkRequests) {
                    if (bulkRequest.requestNumberOfActions() < bulkCommitSize && bulkRequest.tryLock()) {
                        try {
                            bulkRequest.add(eir.build());
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
        public ESBulkRequest add(ESUpdateByQueryRequest esUpdateRequest) {
            ES7xUpdateByQueryRequest eir = (ES7xUpdateByQueryRequest) esUpdateRequest;
            while (true) {
                for (ConcurrentBulkRequest bulkRequest : bulkRequests) {
                    if (bulkRequest.requestNumberOfActions() < bulkCommitSize && bulkRequest.tryLock()) {
                        try {
                            bulkRequest.add(eir.build());
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
        public ES7xBulkRequest add(ESUpdateRequest esUpdateRequest) {
            ES7xUpdateRequest eur = (ES7xUpdateRequest) esUpdateRequest;
            while (true) {
                for (ConcurrentBulkRequest bulkRequest : bulkRequests) {
                    if (bulkRequest.requestNumberOfActions() < bulkCommitSize && bulkRequest.tryLock()) {
                        try {
                            bulkRequest.add(eur.build());
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
        public ES7xBulkRequest add(ESDeleteRequest esDeleteRequest) {
            ES7xDeleteRequest edr = (ES7xDeleteRequest) esDeleteRequest;
            while (true) {
                for (ConcurrentBulkRequest bulkRequest : bulkRequests) {
                    if (bulkRequest.requestNumberOfActions() < bulkCommitSize && bulkRequest.tryLock()) {
                        try {
                            bulkRequest.add(edr.build());
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
            ES7xBulkResponse es7xBulkResponse = new ES7xBulkResponse(bulkResponse);
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
            return es7xBulkResponse;
        }

        private BulkResponse bulk(BulkRequest bulkRequest) throws IOException {
            if (bulkRequest.numberOfActions() == 0) {
                return new BulkResponse(new BulkItemResponse[0], 0L);
            }
            IOException ioException = null;
            int bulkRetryCount = Math.max(0, connection.bulkRetryCount);
            for (int i = 0; i <= bulkRetryCount; i++) {
                try {
                    return connection.restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                } catch (IOException e) {
                    ioException = e;
                    yieldThread();
                }
            }
            throw ioException;
        }

        private BulkRequestResponse commit(ConcurrentBulkRequest bulkRequest, boolean retry) throws IOException {
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
                                bulkRequest.add(errorRequest);
                            }
                        } else {
                            bulkRequest.clear();
                            for (DocWriteRequest<?> errorRequest : errorRequests1) {
                                bulkRequest.add(errorRequest);
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
                ES7xUpdateByQueryRequest updateByQueryRequest = bulkRequest.pollUpdateByQuery();
                if (updateByQueryRequest == null) {
                    break;
                }
                BulkByScrollResponse updatedByQuery = connection.restHighLevelClient.updateByQuery(updateByQueryRequest.updateByQueryRequest, RequestOptions.DEFAULT);
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
                rowList.forEach(bulkRequest::add);
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
            String string = new ArrayList<>(bulkRequests[0].requests()).stream().limit(20).collect(Collectors.toList()).toString();
            string = string.length() > 500 ? string.substring(500) : string;
            return "BulkRequest{" +
                    "size=" + numberOfActions() +
                    ", requests=" + string +
                    '}';
        }
    }
}
