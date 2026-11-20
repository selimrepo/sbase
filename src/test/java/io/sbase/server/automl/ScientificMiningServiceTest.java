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
    void detectsPerfectCategoricalAssociationWithCramersV() {
        List<Map<String,Object>> data = new ArrayList<>();
        for (int i=0;i<40;i++) data.add(Map.of("region",i<20?"north":"south",
                "segment",i<20?"one":"two"));
        Map<String,Object> result=service.associations(Map.of("data",data,
                "features",List.of("region","segment")));
        Map<?,?> association=(Map<?,?>)((List<?>)result.get("associations")).get(0);
        assertEquals("CRAMERS_V_CHI_SQUARE",association.get("method"));
        assertEquals(1.0,((Number)association.get("cramersV")).doubleValue(),1e-9);
        assertTrue(((Number)association.get("pValue")).doubleValue()<0.001);
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

    @Test
    void mannWhitneyDetectsSeparatedIndependentGroups() {
        List<Map<String,Object>> data=new ArrayList<>();
        for(int i=1;i<=12;i++) {
            data.add(Map.of("arm","A","value",i));
            data.add(Map.of("arm","B","value",i+20));
        }
        Map<String,Object> result=new ScientificResearchService().mannWhitneyU(Map.of("data",data,
                "groupColumn","arm","valueColumn","value","groupA","A","groupB","B"));
        assertEquals("MANN_WHITNEY_U_ASYMPTOTIC_TWO_SIDED",result.get("method"));
        assertTrue(((Number)result.get("pValueTwoSided")).doubleValue()<0.001);
        assertTrue(((Number)result.get("rankBiserialCorrelation")).doubleValue()<0);
    }

    @Test
    void kruskalWallisFindsRankDifferenceAcrossThreeGroups() {
        List<Map<String,Object>> data=new ArrayList<>();
        for(int i=1;i<=10;i++) {
            data.add(Map.of("arm","A","value",i));
            data.add(Map.of("arm","B","value",i+20));
            data.add(Map.of("arm","C","value",i+40));
        }
        Map<String,Object> result=new ScientificResearchService().kruskalWallis(Map.of("data",data,
                "groupColumn","arm","valueColumn","value"));
        assertEquals("KRUSKAL_WALLIS_CHI_SQUARE_APPROXIMATION",result.get("method"));
        assertEquals(2,result.get("degreesOfFreedom"));
        assertTrue(((Number)result.get("pValue")).doubleValue()<0.001);
    }

    @Test
    void oneWayAnovaReportsEffectSizeAndOmnibusPValue() {
        List<Map<String,Object>> data=new ArrayList<>();
        for(int i=0;i<15;i++) {
            data.add(Map.of("arm","A","value",(double)i));
            data.add(Map.of("arm","B","value",(double)i+15));
            data.add(Map.of("arm","C","value",(double)i+30));
        }
        Map<String,Object> result=new ScientificResearchService().oneWayAnova(Map.of("data",data,
                "groupColumn","arm","valueColumn","value"));
        assertEquals("ONE_WAY_CLASSICAL_ANOVA",result.get("method"));
        assertTrue(((Number)result.get("pValue")).doubleValue()<0.001);
        assertTrue(((Number)result.get("etaSquared")).doubleValue()>0.5);
    }

    @Test
    void rollingMadDetectsSpikeWithoutUsingCurrentPointInBaseline() {
        List<Map<String,Object>> points=new ArrayList<>();
        for(int i=0;i<12;i++) points.add(Map.of("timestamp","2026-01-"+String.format("%02d",i+1),
                "value",i==11?100.0:10.0));
        Map<String,Object> result=new ScientificResearchService().detectTimeSeriesAnomalies(
                Map.of("points",points,"window",5,"threshold",3.5));
        assertEquals(1,result.get("anomalyCount"));
        Map<?,?> anomaly=(Map<?,?>)((List<?>)result.get("anomalies")).get(0);
        assertEquals(11,anomaly.get("index"));
    }

    @Test
    void propensityWeightingEstimatesKnownSyntheticTreatmentEffect() {
        List<Map<String,Object>> data=new ArrayList<>();
        for(int i=0;i<100;i++) {
            int treated=i%2;
            data.add(Map.of("treatment",treated==1?"yes":"no","age",(double)i,
                    "outcome",3.0*treated+0.5*i));
        }
        Map<String,Object> result=new ScientificResearchService().estimateCausalEffect(Map.of(
                "data",data,"treatmentColumn","treatment","treatedValue","yes",
                "outcomeColumn","outcome","covariates",List.of("age")));
        assertEquals("PROPENSITY_SCORE_INVERSE_PROBABILITY_WEIGHTED_ATE",result.get("method"));
        assertEquals(3.0,((Number)result.get("estimatedAverageTreatmentEffect")).doubleValue(),0.15);
        assertEquals(100,result.get("completeCaseSampleSize"));
    }

    @Test
    void experimentDesignInflatesSampleSizeForAttrition() {
        ScientificResearchService service=new ScientificResearchService();
        Map<String,Object> base=service.designContinuousExperiment(Map.of("standardizedEffect",0.5));
        Map<String,Object> adjusted=service.designContinuousExperiment(Map.of("standardizedEffect",0.5,
                "attritionRate",0.1));
        assertTrue(((Number)adjusted.get("totalRequiredN")).intValue()>
                ((Number)base.get("totalRequiredN")).intValue());
        assertEquals(0.8,((Number)adjusted.get("targetPower")).doubleValue(),1e-9);
    }

}
