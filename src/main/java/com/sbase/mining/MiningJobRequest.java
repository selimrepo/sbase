package com.sbase.mining;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record MiningJobRequest(
        @NotBlank @Pattern(regexp = "[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?")
        String sourceTable,
        @NotBlank @Pattern(regexp = "[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?")
        String outputTable,
        @NotBlank @Pattern(regexp = "describe|profile|kmeans")
        String operation,
        @Min(2) @Max(100) Integer k,
        @Min(1) @Max(500) Integer maxIterations
) { }
