package com.sbase.pipeline;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record PipelineRequest(
        @NotBlank @Size(max = 160) String name,
        @Size(max = 4000) String description,
        @NotBlank String definitionJson,
        boolean enabled
) { }