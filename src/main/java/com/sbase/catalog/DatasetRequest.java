package com.sbase.catalog;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public record DatasetRequest(
        @NotBlank @Size(max = 120) String name,
        @NotBlank @Pattern(regexp = "[A-Za-z_][A-Za-z0-9_]*") @Size(max = 120) String namespace,
        @NotBlank @Pattern(regexp = "[A-Za-z_][A-Za-z0-9_]*") @Size(max = 255) String tableName,
        @Size(max = 4000) String description,
        @NotBlank @Pattern(regexp = "DORIS|POSTGRESQL|S3|PARQUET|CSV") String sourceType,
        String schemaJson
) { }