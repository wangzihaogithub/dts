package com.github.dts.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

public class TypeLlmVectorAPI {
    private static final Logger log = LoggerFactory.getLogger(TypeLlmVectorAPI.class);
    private final ESSyncConfig.ObjectField.ParamLlmVector llmVector;
    private final BlockingQueue<VectorCompletableFuture> futureList;
    private final LlmEmbeddingModel llmEmbeddingModel;
    private final CacheLlmEmbeddingModel cacheLlmEmbeddingModel;

    public TypeLlmVectorAPI(ESSyncConfig.ObjectField.ParamLlmVector llmVector) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        this.llmVector = llmVector;
        this.llmEmbeddingModel = llmVector.getModelClass().getConstructor(ESSyncConfig.ObjectField.ParamLlmVector.class).newInstance(llmVector);
        this.cacheLlmEmbeddingModel = llmVector.isEnableWeakCache() ? new CacheLlmEmbeddingModel(llmEmbeddingModel) : null;
        this.futureList = VectorCompletableFuture.getQueue(llmVector.getRequestQueueName(), llmVector.getRequestMaxContentSize(), llmVector.getQpm());
    }

    public CompletableFuture<float[]> vector(String content) {
        VectorCompletableFuture future = new VectorCompletableFuture(content, this::commit);
        try {
            futureList.put(future);
        } catch (InterruptedException e) {
            Util.sneakyThrows(e);
        }
        if (llmVector.isContentSizeThreshold(futureList.size())) {
            commit();
        }
        return future;
    }

    private void commit() {
        if (this.futureList.isEmpty()) {
            return;
        }
        ArrayList<VectorCompletableFuture> futureList = new ArrayList<>(this.futureList.size());
        this.futureList.drainTo(futureList);
        if (futureList.isEmpty()) {
            return;
        }

        List<String> contentList = new ArrayList<>(futureList.size());
        for (VectorCompletableFuture future : futureList) {
            contentList.add(future.getContent());
        }
        while (true) {
            try {
                List<float[]> vectorList = embedAll(contentList);
                for (int i = 0, size = vectorList.size(); i < size; i++) {
                    float[] vector = vectorList.get(i);
                    VectorCompletableFuture future = futureList.get(i);
                    future.complete(vector);
                }
                break;
            } catch (Exception e) {
                String message = e.getMessage();
                if (message != null && message.contains("limit_requests")) {
                    log.warn("llm queue '{}' , limit_requests {}, next retry ", llmVector.getRequestQueueName(), message);
                    try {
                        Thread.sleep(1000L);
                        // sleep retry
                    } catch (InterruptedException ex) {
                        throw e;
                    }
                } else {
                    throw e;
                }
            }
        }
    }

    private List<float[]> embedAll(List<String> requestList) {
        LlmEmbeddingModel model = cacheLlmEmbeddingModel != null ? cacheLlmEmbeddingModel : llmEmbeddingModel;
        List<String> textSet = new ArrayList<>(new LinkedHashSet<>(requestList));
        List<float[]> embeddingSet = model.embedAll(textSet);

        Map<String, float[]> vectorMap = new HashMap<>(textSet.size());
        for (int i = 0, size = textSet.size(); i < size; i++) {
            vectorMap.put(textSet.get(i), embeddingSet.get(i));
        }
        List<float[]> result = new ArrayList<>(requestList.size());
        for (String request : requestList) {
            result.add(vectorMap.get(request));
        }
        return result;
    }
}
