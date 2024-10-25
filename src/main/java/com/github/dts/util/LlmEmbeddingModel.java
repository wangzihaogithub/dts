package com.github.dts.util;

import java.util.List;

public interface LlmEmbeddingModel {
    List<float[]> embedAll(List<String> contentList);
}
