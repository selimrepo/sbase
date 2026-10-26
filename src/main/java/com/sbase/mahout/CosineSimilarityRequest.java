package com.sbase.mahout;

import jakarta.validation.constraints.NotEmpty;
import java.util.List;

public record CosineSimilarityRequest(
        @NotEmpty List<Double> left,
        @NotEmpty List<Double> right
) { }
