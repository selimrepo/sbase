package io.sbase.server.automl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.ml.*;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.evaluation.*;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.*;
import org.apache.spark.sql.*;

import java.util.*;

/** Spark ML candidate search with deterministic validation and durable PipelineModel output. */
public final class AutoMlService {
    private final SparkSession spark;
    private static final ObjectMapper JSON=new ObjectMapper();
    public AutoMlService(SparkSession spark){this.spark=Objects.requireNonNull(spark);}

    public Map<String,Object> train(Map<String,Object> request){return train(request,null);}

    public Map<String,Object> train(Map<String,Object> request,String modelPath){
        String task=String.valueOf(request.getOrDefault("task","classification")).toLowerCase(Locale.ROOT);
        String label=String.valueOf(request.getOrDefault("label","label"));
        List<String> features=((List<?>)request.getOrDefault("features",List.of())).stream().map(String::valueOf).toList();
        List<?> input=(List<?>)request.getOrDefault("data",List.of());
        if(!Set.of("classification","regression").contains(task))throw new IllegalArgumentException("task must be classification or regression");
        if(features.isEmpty()||input.size()<50)throw new IllegalArgumentException("AutoML requires features and at least 50 rows");
        if(input.size()>100000)throw new IllegalArgumentException("AutoML inline data limit is 100000 rows");
        if(new HashSet<>(features).size()!=features.size()||features.contains(label))throw new IllegalArgumentException("Feature columns must be unique and cannot include label");
        List<String> jsonRows=new ArrayList<>();
        for(Object row:input){
            if(!(row instanceof Map<?,?> map)||!map.containsKey(label))throw new IllegalArgumentException("Every row must contain label");
            for(String feature:features)if(!map.containsKey(feature))throw new IllegalArgumentException("Missing feature: "+feature);
            jsonRows.add(JSON.valueToTree(row).toString());
        }
        Dataset<Row> raw=spark.read().json(spark.createDataset(jsonRows,Encoders.STRING()));
        Set<String> columns=new HashSet<>(Arrays.asList(raw.columns()));
        if(!columns.contains(label)||!columns.containsAll(features))throw new IllegalArgumentException("Unknown label or feature column");
        VectorAssembler assembler=new VectorAssembler().setInputCols(features.toArray(String[]::new))
                .setOutputCol("features").setHandleInvalid("skip");
        Dataset<Row>[] split=raw.randomSplit(new double[]{0.8,0.2},42L);
        Dataset<Row> train=split[0],validation=split[1];
        if(train.limit(2).count()<2||validation.limit(2).count()<2)throw new IllegalArgumentException("Not enough rows after validation split");
        List<PipelineStage> candidates=new ArrayList<>();
        if(task.equals("classification")){
            candidates.add(new LogisticRegression().setLabelCol(label).setFeaturesCol("features").setMaxIter(100).setRegParam(0.01));
            candidates.add(new RandomForestClassifier().setLabelCol(label).setFeaturesCol("features").setNumTrees(100).setMaxDepth(8).setSeed(42));
            candidates.add(new DecisionTreeClassifier().setLabelCol(label).setFeaturesCol("features").setMaxDepth(8).setSeed(42));
        }else{
            candidates.add(new LinearRegression().setLabelCol(label).setFeaturesCol("features").setMaxIter(100).setRegParam(0.01));
            candidates.add(new RandomForestRegressor().setLabelCol(label).setFeaturesCol("features").setNumTrees(100).setMaxDepth(8).setSeed(42));
            candidates.add(new DecisionTreeRegressor().setLabelCol(label).setFeaturesCol("features").setMaxDepth(8).setSeed(42));
        }
        int max=Math.max(1,Math.min(3,((Number)request.getOrDefault("maxCandidates",3)).intValue()));
        boolean classification=task.equals("classification");
        String metric=classification?"accuracy":"rmse";
        double bestScore=classification?Double.NEGATIVE_INFINITY:Double.POSITIVE_INFINITY;
        PipelineModel best=null;String bestAlgorithm="";
        List<Map<String,Object>> trials=new ArrayList<>();
        for(PipelineStage estimator:candidates.stream().limit(max).toList()){
            String algorithm=estimator.getClass().getSimpleName();long started=System.nanoTime();
            try{
                PipelineModel model=new Pipeline().setStages(new PipelineStage[]{assembler,estimator}).fit(train);
                Dataset<Row> predictions=model.transform(validation);
                double score=classification
                        ?new MulticlassClassificationEvaluator().setLabelCol(label).setPredictionCol("prediction").setMetricName(metric).evaluate(predictions)
                        :new RegressionEvaluator().setLabelCol(label).setPredictionCol("prediction").setMetricName(metric).evaluate(predictions);
                trials.add(Map.of("algorithm",algorithm,"metric",metric,"score",score,"status","SUCCEEDED",
                        "durationMs",TimeUnitNanos.toMillis(System.nanoTime()-started)));
                if(classification?score>bestScore:score<bestScore){best=model;bestScore=score;bestAlgorithm=algorithm;}
            }catch(Exception error){
                trials.add(Map.of("algorithm",algorithm,"status","FAILED","durationMs",TimeUnitNanos.toMillis(System.nanoTime()-started),
                        "error",error.getClass().getSimpleName()+": "+String.valueOf(error.getMessage())));
            }
        }
        if(best==null)throw new IllegalStateException("All AutoML candidates failed: "+trials);
        String modelUri=null;
        if(modelPath!=null&&!modelPath.isBlank()){
            try{best.write().overwrite().save(modelPath);modelUri=modelPath;}
            catch(Exception error){throw new IllegalStateException("Model persistence failed: "+error.getMessage(),error);}
        }
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("status","SUCCEEDED");result.put("task",task);result.put("algorithm",bestAlgorithm);
        result.put("metric",metric);result.put("score",bestScore);result.put("trainingRows",train.count());
        result.put("validationRows",validation.count());result.put("features",features);result.put("trials",trials);
        result.put("modelFormat","Spark PipelineModel");if(modelUri!=null)result.put("modelUri",modelUri);
        return result;
    }
    private static final class TimeUnitNanos{
        static long toMillis(long nanos){return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(nanos);}
    }
}
