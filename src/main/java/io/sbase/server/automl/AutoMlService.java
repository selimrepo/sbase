package io.sbase.server.automl;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.ml.*;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.regression.*;
import org.apache.spark.ml.evaluation.*;
import org.apache.spark.ml.tuning.*;
import org.apache.spark.ml.param.ParamMap;

import java.util.*;

/**
 * Bounded Spark ML AutoML search. It performs a deterministic holdout evaluation
 * over a small, explicit candidate set and returns the winning model metadata.
 */
public final class AutoMlService {
    private final SparkSession spark;
    public AutoMlService(SparkSession spark) { this.spark = Objects.requireNonNull(spark); }

    public Map<String,Object> train(Map<String,Object> request) {
        String task=String.valueOf(request.getOrDefault("task","classification")).toLowerCase(Locale.ROOT);
        String label=String.valueOf(request.getOrDefault("label","label"));
        List<String> features=((List<?>)request.getOrDefault("features",List.of())).stream().map(String::valueOf).toList();
        List<?> input=(List<?>)request.getOrDefault("data",List.of());
        if(!Set.of("classification","regression").contains(task)) throw new IllegalArgumentException("task must be classification or regression");
        if(features.isEmpty() || input.size()<30) throw new IllegalArgumentException("AutoML requires at least one feature and 30 rows");
        if(input.size()>100000) throw new IllegalArgumentException("AutoML input is limited to 100000 rows per request");
        List<String> jsonRows=new ArrayList<>();
        for(Object row:input) {
            if(!(row instanceof Map<?,?> map) || !map.containsKey(label)) throw new IllegalArgumentException("Every row must contain the label column");
            for(String feature:features) if(!map.containsKey(feature)) throw new IllegalArgumentException("Missing feature column: "+feature);
            jsonRows.add(new com.fasterxml.jackson.databind.ObjectMapper().valueToTree(row).toString());
        }
        Dataset<Row> raw=spark.read().json(spark.createDataset(jsonRows, Encoders.STRING()));
        for(String column:features) if(!raw.columnsAsList().contains(column)) throw new IllegalArgumentException("Unknown feature: "+column);
        if(!Arrays.asList(raw.columns()).contains(label)) throw new IllegalArgumentException("Unknown label: "+label);
        Dataset<Row>[] split=raw.randomSplit(new double[]{0.8,0.2},42L);
        Dataset<Row> train=split[0], test=split[1];
        VectorAssembler assembler=new VectorAssembler().setInputCols(features.toArray(String[]::new)).setOutputCol("features").setHandleInvalid("skip");
        List<Map<String,Object>> trials=new ArrayList<>();
        PipelineModel best=null; double bestScore=task.equals("classification")?Double.NEGATIVE_INFINITY:Double.POSITIVE_INFINITY;
        String bestAlgorithm="";
        List<PipelineStage> candidates=new ArrayList<>();
        if(task.equals("classification")) {
            candidates.add(new LogisticRegression().setLabelCol(label).setFeaturesCol("features").setMaxIter(100).setRegParam(0.01));
            candidates.add(new RandomForestClassifier().setLabelCol(label).setFeaturesCol("features").setNumTrees(80).setMaxDepth(8).setSeed(42));
            candidates.add(new DecisionTreeClassifier().setLabelCol(label).setFeaturesCol("features").setMaxDepth(8).setSeed(42));
        } else {
            candidates.add(new LinearRegression().setLabelCol(label).setFeaturesCol("features").setMaxIter(100).setRegParam(0.01));
            candidates.add(new RandomForestRegressor().setLabelCol(label).setFeaturesCol("features").setNumTrees(80).setMaxDepth(8).setSeed(42));
            candidates.add(new DecisionTreeRegressor().setLabelCol(label).setFeaturesCol("features").setMaxDepth(8).setSeed(42));
        }
        int max=Math.max(1,Math.min(3,((Number)request.getOrDefault("maxCandidates",3)).intValue()));
        for(PipelineStage estimator:candidates.stream().limit(max).toList()) {
            String algorithm=estimator.getClass().getSimpleName();
            try {
                Pipeline pipeline=new Pipeline().setStages(new PipelineStage[]{assembler,estimator});
                PipelineModel model=pipeline.fit(train);
                Dataset<Row> predictions=model.transform(test);
                double score;
                String metric;
                if(task.equals("classification")) {
                    score=new MulticlassClassificationEvaluator().setLabelCol(label).setPredictionCol("prediction").setMetricName("accuracy").evaluate(predictions);
                    metric="accuracy";
                } else {
                    score=new RegressionEvaluator().setLabelCol(label).setPredictionCol("prediction").setMetricName("rmse").evaluate(predictions);
                    metric="rmse";
                }
                trials.add(Map.of("algorithm",algorithm,"metric",metric,"score",score,"status","SUCCEEDED"));
                boolean better=task.equals("classification")?score>bestScore:score<bestScore;
                if(better){best=model;bestScore=score;bestAlgorithm=algorithm;}
            } catch(Exception error) {
                trials.add(Map.of("algorithm",algorithm,"status","FAILED","error",error.getClass().getSimpleName()+": "+String.valueOf(error.getMessage())));
            }
        }
        if(best==null) throw new IllegalStateException("All AutoML candidates failed: "+trials);
        return Map.of("status","SUCCEEDED","task",task,"algorithm",bestAlgorithm,"metric",task.equals("classification")?"accuracy":"rmse",
                "score",bestScore,"trainingRows",train.count(),"validationRows",test.count(),
                "features",features,"trials",trials,"modelFormat","Spark PipelineModel (in-memory; persistence endpoint is separate)");
    }
}
