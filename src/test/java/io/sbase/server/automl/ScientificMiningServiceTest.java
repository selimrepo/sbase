package io.sbase.server.automl;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ScientificMiningServiceTest {
    private final ScientificMiningService service = new ScientificMiningService();

    @Test
    void reportsNumericAndMixedAssociationsWithFdrAdjustedValues() {
        List<Map<String,Object>> data = new ArrayList<>();
        for (int i=0;i<40;i++) {
            Map<String,Object> row = new LinkedHashMap<>();
            row.put("x", i); row.put("y", i*3); row.put("segment", i<20 ? "A" : "B");
            data.add(row);
        }
        Map<String,Object> result = service.associations(Map.of("data",data,"features",List.of("x","y","segment")));
        List<?> associations = (List<?>) result.get("associations");
        assertEquals(3, associations.size());
        Map<?,?> numeric = (Map<?,?>) associations.get(0);
        assertEquals("PEARSON_AND_SPEARMAN", numeric.get("method"));
        assertEquals(1.0, ((Number)numeric.get("spearmanRho")).doubleValue(), 1e-9);
        for (Object value : associations) {
            Map<?,?> item=(Map<?,?>)value;
            assertTrue(((Number)item.get("pValue")).doubleValue() >= 0);
            assertTrue(((Number)item.get("qValue")).doubleValue() <= 1);
        }
    }

    @Test
    void selectsMonotonicFeaturesAndExcludesConstantFeatures() {
        List<Map<String,Object>> data = new ArrayList<>();
        for (int i=0;i<50;i++) data.add(Map.of("signal",i,"constant",1,"target",i*2.0));
        Map<String,Object> result = service.selectFeatures(Map.of("data",data,"target","target",
                "features",List.of("signal","constant"),"fdrAlpha",0.05,"minimumAbsoluteSpearman",0.5));
        assertEquals(List.of("signal"), result.get("selectedFeatures"));
        List<?> ranked=(List<?>)result.get("rankedFeatures");
        assertEquals("CONSTANT_FEATURE", ((Map<?,?>)ranked.get(1)).get("status"));
    }

    @Test
    void welchTestReportsEffectSizeAndConfidenceInterval() {
        List<Map<String,Object>> data = new ArrayList<>();
        for(int i=0;i<20;i++){
            data.add(Map.of("arm", "control", "outcome", (double)i));
            data.add(Map.of("arm", "treatment", "outcome", (double)i+12));
        }
        Map<String,Object> result=service.compareGroups(Map.of("data",data,"groupColumn","arm",
                "valueColumn","outcome","groupA","treatment","groupB","control"));
        assertEquals("WELCH_TWO_SAMPLE_T_TEST",result.get("method"));
        assertTrue(((Number)result.get("pValueTwoSided")).doubleValue()<0.001);
        assertEquals(12.0,((Number)result.get("meanDifferenceAminusB")).doubleValue(),1e-9);
        Map<?,?> ci=(Map<?,?>)result.get("confidenceInterval");
        assertTrue(((Number)ci.get("lower")).doubleValue()<12.0);
        assertTrue(((Number)ci.get("upper")).doubleValue()>12.0);
        assertNotNull(result.get("hedgesG"));
    }

    @Test
    void rejectsInvalidGroupSamples() {
        List<Map<String,Object>> data=List.of(Map.of("arm","A","value",1));
        assertThrows(IllegalArgumentException.class,()->service.compareGroups(Map.of("data",data,
                "groupColumn","arm","valueColumn","value","groupA","A","groupB","B")));
    }
}
