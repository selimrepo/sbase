package com.sbase.mahout;

import org.apache.mahout.math.DenseVector;
import org.springframework.stereotype.Service;

/**
 * Mahout-backed vector primitives used by recommendation and similarity pipelines.
 * Higher-level algorithms can be composed behind this service without coupling REST APIs
 * to Mahout's numerical data structures.
 */
@Service
public class MahoutVectorService {
    public double cosineSimilarity(double[] left, double[] right) {
        if (left == null || right == null || left.length == 0 || left.length != right.length) {
            throw new IllegalArgumentException("Vectors must be non-empty and have equal dimensions");
        }
        DenseVector a = new DenseVector(left);
        DenseVector b = new DenseVector(right);
        double denominator = a.norm(2) * b.norm(2);
        return denominator == 0.0 ? 0.0 : a.dot(b) / denominator;
    }
}
