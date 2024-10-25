package com.github.dts.util;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

public class TypeLlmVectorAPI {
    private final ESSyncConfig.ObjectField.ParamLlmVector llmVector;
    private final BlockingQueue<VectorCompletableFuture> futureList;
    private final LlmEmbeddingModel llmEmbeddingModel;

    public TypeLlmVectorAPI(ESSyncConfig.ObjectField.ParamLlmVector llmVector) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        this.llmVector = llmVector;
        this.llmEmbeddingModel = llmVector.getModelClass().getConstructor(ESSyncConfig.ObjectField.ParamLlmVector.class).newInstance(llmVector);
        this.futureList = new LinkedBlockingQueue<>(Math.max(1, llmVector.getRequestMaxContentSize()));
    }

    public static void beforeBulk(Object params) {
        List<Object> stack = new ArrayList<>();
        stack.add(params);
        while (!stack.isEmpty()) {
            Object remove = stack.remove(stack.size() - 1);
            if (remove instanceof Map) {
                for (Map.Entry<String, Object> entry : ((Map<String, Object>) remove).entrySet()) {
                    Object value = entry.getValue();
                    if (isCompletableFuture(value)) {
                        entry.setValue(getFuture(value));
                    } else {
                        stack.add(value);
                    }
                }
            } else if (remove instanceof List) {
                List list = (List<?>) remove;
                int i = 0;
                for (Object item : list) {
                    if (isCompletableFuture(item)) {
                        list.set(i, getFuture(item));
                    } else {
                        stack.add(item);
                    }
                }
            }
        }
    }

    private static boolean isCompletableFuture(Object data) {
        return data instanceof CompletableFuture;
    }

    private static Object getFuture(Object data) {
        try {
            return ((CompletableFuture<?>) data).get();
        } catch (Exception e) {
            Util.sneakyThrows(e);
            return null;
        }
    }

    public CompletableFuture<float[]> vector(String content) {
        VectorCompletableFuture future = new VectorCompletableFuture(content, this::commit);
        futureList.add(future);
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
        List<float[]> vectorList = llmEmbeddingModel.embedAll(contentList);
        for (int i = 0, size = vectorList.size(); i < size; i++) {
            float[] vector = vectorList.get(i);
            VectorCompletableFuture future = futureList.get(i);
            future.complete(vector);
        }
    }
}
