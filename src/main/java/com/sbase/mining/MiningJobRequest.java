package com.sbase.mining;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record MiningJobRequest(
        @NotBlank String sourceTable,
        @NotBlank String outputTable,
        @Pattern(regexp = "describe|profile|kmeans", message = "Supported operations: describe, profile, kmeans")
        String operation,
        Integer k,
        Integer maxIterations
) { }
