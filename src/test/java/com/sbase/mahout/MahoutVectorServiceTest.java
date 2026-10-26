package com.sbase.mahout;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MahoutVectorServiceTest {
    private final MahoutVectorService service = new MahoutVectorService();

    @Test
    void calculatesCosineSimilarity() {
        assertEquals(1.0, service.cosineSimilarity(new double[]{1, 0}, new double[]{2, 0}), 1e-9);
        assertEquals(0.0, service.cosineSimilarity(new double[]{1, 0}, new double[]{0, 1}), 1e-9);
    }

    @Test
    void zeroVectorHasZeroSimilarity() {
        assertEquals(0.0, service.cosineSimilarity(new double[]{0, 0}, new double[]{1, 2}), 1e-9);
    }

    @Test
    void rejectsDifferentDimensions() {
        assertThrows(IllegalArgumentException.class,
                () -> service.cosineSimilarity(new double[]{1}, new double[]{1, 2}));
    }
}
