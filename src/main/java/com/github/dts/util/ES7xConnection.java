package com.github.dts.util;


import com.google.common.collect.Lists;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
    private static final Logger logger = LoggerFactory.getLogger(ES7xConnection.class);
    private final RestHighLevelClient restHighLevelClient;
    private final int concurrentBulkRequest;
    private final int bulkCommitSize;
    private final int maxRetryCount;
    private final Map<String, CompletableFuture<ESBulkRequest.EsRefreshResponse>> refreshAsyncCache = new ConcurrentHashMap<>();

    public ES7xConnection(CanalConfig.OuterAdapterConfig.Es7x es7x) {
        String[] elasticsearchUri = es7x.getAddress();
        HttpHost[] httpHosts = Arrays.stream(elasticsearchUri).map(HttpHost::create).toArray(HttpHost[]::new);
        String name = es7x.getUsername();
        String pwd = es7x.getPassword();
        String clusterName = es7x.getProperties() != null ? es7x.getProperties().get("cluster.name") : null;
        int concurrentBulkRequest = es7x.getConcurrentBulkRequest();

        final RestClientBuilder clientBuilder = RestClient
                .builder(httpHosts)
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                        .setConnectTimeout(10 * 60 * 60)
                        .setConnectionRequestTimeout(10 * 60 * 60)
                        .setSocketTimeout(10 * 60 * 60))
                .setHttpClientConfigCallback(httpClientBuilder -> {
                    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(name, pwd));
                    httpClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider)
                            .setMaxConnTotal(concurrentBulkRequest)
                            .setMaxConnPerRoute(concurrentBulkRequest);
                    return httpClientBuilder
                            .setDefaultIOReactorConfig(IOReactorConfig.custom().setSoKeepAlive(true).build())
                            .setKeepAliveStrategy((response, context) -> TimeUnit.MINUTES.toMillis(3000));
                });
        if (clusterName != null && !clusterName.isEmpty()) {
            clientBuilder.setPathPrefix(clusterName);
        }
        this.maxRetryCount = es7x.getMaxRetryCount();
        this.bulkCommitSize = es7x.getBulkCommitSize();
        this.concurrentBulkRequest = concurrentBulkRequest;
        this.restHighLevelClient = new RestHighLevelClient(clientBuilder);
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
        return refreshAsyncCache.computeIfAbsent(String.join(",", indices), key -> {
            CompletableFuture<ESBulkRequest.EsRefreshResponse> future = new CompletableFuture<>();
            RefreshRequest request = new RefreshRequest(indices);
            Cancellable cancellable = restHighLevelClient.indices().refreshAsync(request, RequestOptions.DEFAULT, new ActionListener<RefreshResponse>() {
                @Override
                public void onResponse(RefreshResponse refreshResponse) {
                    refreshAsyncCache.remove(key);
                    future.complete(new Es7RefreshResponse(refreshResponse));
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

    public MappingMetaData getMapping(String index) {
        MappingMetaData mappingMetaData = null;
        Map<String, MappingMetaData> mappings = Collections.emptyMap();
        IOException ioException = null;
        for (int i = 0, retry = 3; i < retry; i++) {
            try {
                GetMappingsRequest request = new GetMappingsRequest();
                request.indices(index);
                GetMappingsResponse response = restHighLevelClient.indices()
                        .getMapping(request, RequestOptions.DEFAULT);

                mappings = response.mappings();
                break;
            } catch (IOException e) {
                ioException = e;
            } catch (NullPointerException e) {
                throw new IllegalArgumentException("Not found the mapping info of index: " + index);
            }
        }
        if (ioException != null) {
            logger.error("getMapping error {}", ioException, ioException);
            Util.sneakyThrows(ioException);
            return null;
        }

        for (String key : mappings.keySet()) {
            if (key.startsWith(index)) {
                mappingMetaData = mappings.get(key);
                break;
            }
        }
        if (mappingMetaData == null && !mappings.isEmpty()) {
            return mappings.values().iterator().next();
        }
        return mappingMetaData;
    }

    public static class Es7RefreshResponse implements ESBulkRequest.EsRefreshResponse {
        private final RefreshResponse refreshResponse;

        public Es7RefreshResponse(RefreshResponse refreshResponse) {
            this.refreshResponse = refreshResponse;
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
                size += requestResponse.response.getItems().length;
            }
            return size;
        }

        @Override
        public boolean hasFailures() {
            for (BulkRequestResponse bulkItemResponses : bulkResponse) {
                if (bulkItemResponses.response.hasFailures()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public boolean isEmpty() {
            for (BulkRequestResponse requestResponse : bulkResponse) {
                if (requestResponse.response.getItems().length > 0) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void processFailBulkResponse(String errorMsg) {
            Set<String> errorList = null;
            int notfound = 0;
            for (BulkRequestResponse bulkItemResponses : bulkResponse) {
                for (BulkItemResponse itemResponse : bulkItemResponses.response.getItems()) {
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
                    } else {
                        if (errorList == null) {
                            errorList = new LinkedHashSet<>();
                        }
                        Exception cause = itemResponse.getFailure().getCause();
                        errorList.add("[" + itemResponse.getOpType() + "]. " + cause.getClass() + ": " + itemResponse.getFailure());
                    }
                }
            }
            if (errorList != null && !errorList.isEmpty()) {
                throw new RuntimeException(errorMsg + "[" + String.join(",\n", errorList) + "]");
            }
        }
    }

    public static class ES7xIndexRequest implements ESBulkRequest.ESIndexRequest {

        private final IndexRequest indexRequest;

        public ES7xIndexRequest(String index, String id) {
            indexRequest = new IndexRequest(index);
            indexRequest.id(id);
        }

        @Override
        public String toString() {
            return indexRequest.toString();
        }

        @Override
        public ES7xIndexRequest setSource(Map<String, ?> source) {
            indexRequest.source(source);
            return this;
        }

        @Override
        public ES7xIndexRequest setRouting(String routing) {
            indexRequest.routing(routing);
            return this;
        }

    }

    public static class ES7xUpdateRequest implements ESBulkRequest.ESUpdateRequest {

        private final UpdateRequest updateRequest;

        public ES7xUpdateRequest(String index, String id) {
            updateRequest = new UpdateRequest(index, id);
        }

        @Override
        public String toString() {
            return updateRequest.toString();
        }

        @Override
        public ES7xUpdateRequest setScript(Script script) {
            updateRequest.script(script);
            return this;
        }

        @Override
        public ES7xUpdateRequest setDoc(Map source) {
            updateRequest.doc(source);
            return this;
        }

        @Override
        public ES7xUpdateRequest setDocAsUpsert(boolean shouldUpsertDoc) {
            updateRequest.docAsUpsert(shouldUpsertDoc);
            return this;
        }

        @Override
        public ES7xUpdateRequest setRouting(String routing) {
            updateRequest.routing(routing);
            return this;
        }

    }

    public static class ES7xDeleteRequest implements ESBulkRequest.ESDeleteRequest {

        private final DeleteRequest deleteRequest;

        public ES7xDeleteRequest(String index, String id) {
            deleteRequest = new DeleteRequest(index, id);
        }

        @Override
        public String toString() {
            return deleteRequest.toString();
        }
    }

    public static class ConcurrentBulkRequest extends BulkRequest {
        private final ReentrantLock lock = new ReentrantLock();

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
        }

        @Override
        public String toString() {
            return "ConcurrentBulkRequest{" +
                    "size=" + numberOfActions() +
                    ", lock=" + lock +
                    '}';
        }
    }

    public static class BulkRequestResponse {
        private final ConcurrentBulkRequest request;
        private BulkResponse response;

        public BulkRequestResponse(ConcurrentBulkRequest request) {
            this.request = request;
        }
    }

    public static class ESSearchRequest {
        private final SearchRequest searchRequest;

        public ESSearchRequest(String index) {
            searchRequest = new SearchRequest(index);
        }

        public ESSearchRequest setQuery(QueryBuilder queryBuilder) {
            searchRequest.source().query(queryBuilder);
            return this;
        }

        public ESSearchRequest size(int size) {
            searchRequest.source().size(size);
            return this;
        }

        public ESSearchRequest fetchSource(String... includes) {
            searchRequest.source().fetchSource(includes, null);
            return this;
        }

        public SearchResponse getResponse(ES7xConnection es7xConnection) {
            try {
                return es7xConnection.restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                Util.sneakyThrows(e);
                return null;
            }
        }
    }

    public static class ES7xBulkRequest implements ESBulkRequest {
        private final ConcurrentBulkRequest[] bulkRequests;
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
                bulkRequests[i] = new ConcurrentBulkRequest();
            }
        }

        private static void yieldThread() {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Util.sneakyThrows(e);
            }
        }

        @Override
        public ESBulkRequest add(Collection<ESRequest> requests) {
            if (requests.isEmpty()) {
                return this;
            }
            ArrayList<ConcurrentBulkRequest> shuffle = new ArrayList<>(Arrays.asList(bulkRequests));
            Collections.shuffle(shuffle);
            while (true) {
                for (ConcurrentBulkRequest bulkRequest : shuffle) {
                    if (bulkRequest.numberOfActions() < bulkCommitSize && bulkRequest.tryLock()) {
                        try {
                            for (Object request : requests) {
                                if (request instanceof ES7xIndexRequest) {
                                    bulkRequest.add(((ES7xIndexRequest) request).indexRequest);
                                } else if (request instanceof ES7xUpdateRequest) {
                                    bulkRequest.add(((ES7xUpdateRequest) request).updateRequest);
                                } else if (request instanceof ES7xDeleteRequest) {
                                    bulkRequest.add(((ES7xDeleteRequest) request).deleteRequest);
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
                    if (bulkRequest.numberOfActions() < bulkCommitSize && bulkRequest.tryLock()) {
                        try {
                            bulkRequest.add(eir.indexRequest);
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
                    if (bulkRequest.numberOfActions() < bulkCommitSize && bulkRequest.tryLock()) {
                        try {
                            bulkRequest.add(eur.updateRequest);
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
                    if (bulkRequest.numberOfActions() < bulkCommitSize && bulkRequest.tryLock()) {
                        try {
                            bulkRequest.add(edr.deleteRequest);
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
                count += bulkRequest.numberOfActions();
            }
            return count;
        }

        @Override
        public boolean isEmpty() {
            for (ConcurrentBulkRequest bulkRequest : bulkRequests) {
                if (bulkRequest.numberOfActions() > 0) {
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
                if (bulkRequest.numberOfActions() > 0 && bulkRequest.tryLock()) {
                    try {
                        BulkRequestResponse requestResponse = commit(bulkRequest, true);
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

        private BulkRequestResponse commit(ConcurrentBulkRequest bulkRequest, boolean retry) throws IOException {
            BulkRequestResponse requestResponse = new BulkRequestResponse(bulkRequest);
            requestResponse.response = connection.restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (retry && requestResponse.response.hasFailures()) {
                if (bulkRequest.numberOfActions() > 1) {
                    ArrayList<DocWriteRequest<?>> errorRequests1 = new ArrayList<>();
                    ArrayList<DocWriteRequest<?>> errorRequests2 = new ArrayList<>();
                    retryAndGetErrorRequests(Collections.unmodifiableList(bulkRequest.requests()), errorRequests1);
                    retryAndGetErrorRequests(Collections.unmodifiableList(errorRequests1), errorRequests2);
                    bulkRequest.clear();
                    for (DocWriteRequest<?> errorRequest : errorRequests2) {
                        bulkRequest.add(errorRequest);
                    }
                }
            } else {
                bulkRequest.clear();
            }
            return requestResponse;
        }

        private void retryAndGetErrorRequests(List<DocWriteRequest<?>> readonlyRequests, List<DocWriteRequest<?>> errorRequests) throws IOException {
            if (readonlyRequests.isEmpty()) {
                return;
            }
            List<List<DocWriteRequest<?>>> partition = Lists.partition(readonlyRequests, (readonlyRequests.size() + 1) / 2);
            for (List<DocWriteRequest<?>> rowList : partition) {
                BulkRequest bulkRequest = new BulkRequest();
                rowList.forEach(bulkRequest::add);
                BulkResponse bulkItemResponses = connection.restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                if (bulkItemResponses.hasFailures()) {
                    if (rowList.size() == 1) {
                        DocWriteRequest<?> retry = readonlyRequests.get(0);
                        int count = retryCounter.computeIfAbsent(retry, e -> 1);
                        if (count < maxRetryCount * 2) {
                            retryCounter.put(retry, count + 1);
                            errorRequests.add(retry);
                        } else {
                            retryCounter.remove(retry);
                        }
                    } else {
                        retryAndGetErrorRequests(rowList, errorRequests);
                    }
                }
            }
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
