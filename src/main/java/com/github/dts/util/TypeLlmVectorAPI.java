package com.github.dts.util;

import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import dev.langchain4j.model.output.Response;

public class TypeLlmVectorAPI {
    private final ESSyncConfig.ObjectField.ParamLlmVector llmVector;
    private final Metadata metadata = new Metadata();
    private volatile transient EmbeddingModel embeddingModel;

    public TypeLlmVectorAPI(ESSyncConfig.ObjectField.ParamLlmVector llmVector) {
        this.llmVector = llmVector;

    }

    public float[] vector(String content) {
        EmbeddingModel embedding = getEmbedding();
        Response<Embedding> embed = embedding.embed(TextSegment.from(content, metadata));
        return embed.content().vector();
    }

    private EmbeddingModel getEmbedding() {
        if (embeddingModel == null) {
            synchronized (this) {
                if (embeddingModel == null) {
                    if (llmVector.getType() == ESSyncConfig.ObjectField.ParamLlmVector.LlmVectorType.openAi) {
                        embeddingModel = OpenAiEmbeddingModel.builder()
                                .apiKey(llmVector.getApiKey())
                                .baseUrl(llmVector.getBaseUrl())
                                .modelName(llmVector.getModelName())
                                .dimensions(llmVector.getDimensions())
                                .build();
                    }
                }
            }
        }
        return embeddingModel;
    }

}
