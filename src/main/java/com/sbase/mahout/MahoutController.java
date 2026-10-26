package com.sbase.mahout;

import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;
import java.util.Map;

@RestController
@RequestMapping("/api/mahout")
public class MahoutController {
    private final MahoutVectorService vectors;

    public MahoutController(MahoutVectorService vectors) { this.vectors = vectors; }

    @PostMapping("/similarity/cosine")
    public Map<String, Double> cosineSimilarity(@Valid @RequestBody CosineSimilarityRequest request) {
        double[] left = request.left().stream().mapToDouble(Double::doubleValue).toArray();
        double[] right = request.right().stream().mapToDouble(Double::doubleValue).toArray();
        return Map.of("cosineSimilarity", vectors.cosineSimilarity(left, right));
    }
}
