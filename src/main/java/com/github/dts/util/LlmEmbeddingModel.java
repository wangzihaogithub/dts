package com.github.dts.util;

import java.util.List;

public interface LlmEmbeddingModel {
    ESSyncConfig.ObjectField.ParamLlmVector getConfig();

    List<float[]> embedAll(List<String> contentList);
}
