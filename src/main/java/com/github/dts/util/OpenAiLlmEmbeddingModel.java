package com.github.dts.util;

import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import dev.langchain4j.model.output.Response;

import java.util.ArrayList;
import java.util.List;

public class OpenAiLlmEmbeddingModel implements LlmEmbeddingModel {
    private final Metadata metadata = new Metadata();
    private final OpenAiEmbeddingModel embeddingModel;

    public OpenAiLlmEmbeddingModel(ESSyncConfig.ObjectField.ParamLlmVector llmVector) {
        embeddingModel = OpenAiEmbeddingModel.builder()
                .apiKey(llmVector.getApiKey())
                .baseUrl(llmVector.getBaseUrl())
                .modelName(llmVector.getModelName())
                .dimensions(llmVector.getDimensions())
                .build();
    }

    @Override
    public List<float[]> embedAll(List<String> contentList) {
        List<TextSegment> textSegmentList = new ArrayList<>(contentList.size());
        for (String content : contentList) {
            textSegmentList.add(TextSegment.from(content, metadata));
        }
        Response<List<Embedding>> embeddedAll = embeddingModel.embedAll(textSegmentList);
        List<Embedding> embeddingList = embeddedAll.content();
        List<float[]> vectorList = new ArrayList<>(embeddingList.size());
        for (Embedding embedding : embeddingList) {
            vectorList.add(embedding.vector());
        }
        return vectorList;
    }
}
