package io.sbase.server.automl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.ml.*;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.evaluation.*;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.regression.*;
import org.apache.spark.ml.tuning.CrossValidator;
import org.apache.spark.ml.tuning.CrossValidatorModel;
import org.apache.spark.ml.tuning.ParamGridBuilder;
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
        boolean timeSeries=task.equals("time_series");
        List<String> droppedFeatures=new ArrayList<>();
        if(timeSeries){
            String timeColumn=String.valueOf(request.getOrDefault("timeColumn",""));
            int lags=Math.max(1,Math.min(24,((Number)request.getOrDefault("lags",3)).intValue()));
            if(timeColumn.isBlank()||input.isEmpty())throw new IllegalArgumentException("time_series requires timeColumn and data");
            List<Map<String,Object>> ordered=new ArrayList<>();
            for(Object row:input){if(!(row instanceof Map<?,?> map)||!map.containsKey(timeColumn)||!(map.get(label) instanceof Number))throw new IllegalArgumentException("time_series rows require timeColumn and numeric label"); Map<String,Object> copy=new LinkedHashMap<>();map.forEach((k,v)->copy.put(String.valueOf(k),v));ordered.add(copy);}
            ordered.sort(Comparator.comparing(row->String.valueOf(row.get(timeColumn))));
            List<Map<String,Object>> lagged=new ArrayList<>();
            List<String> expanded=new ArrayList<>(features);
            for(int i=lags;i<ordered.size();i++){Map<String,Object> row=new LinkedHashMap<>(ordered.get(i));for(int lag=1;lag<=lags;lag++){String column=label+"__lag_"+lag;row.put(column,((Number)ordered.get(i-lag).get(label)).doubleValue());if(!expanded.contains(column))expanded.add(column);}lagged.add(row);}
            features=expanded; input=lagged;
        }
        if(!Set.of("classification","regression","time_series").contains(task))throw new IllegalArgumentException("task must be classification, regression or time_series");
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
        List<String> selectedFeatures=new ArrayList<>();
        double varianceThreshold=Math.max(0.0,((Number)request.getOrDefault("varianceThreshold",1.0e-12)).doubleValue());
        Set<String> numericTypes=Set.of("byte","short","integer","long","float","double","decimal");
        for(String feature:features){
            String type=raw.schema().apply(feature).dataType().typeName().toLowerCase(Locale.ROOT);
            boolean useful;
            if(numericTypes.contains(type)){
                Row stats=raw.agg(org.apache.spark.sql.functions.variance(org.apache.spark.sql.functions.col(feature))).first();
                useful=!stats.isNullAt(0)&&stats.getDouble(0)>varianceThreshold;
            }else{
                useful=raw.select(org.apache.spark.sql.functions.col(feature)).distinct().limit(2).count()>1;
            }
            if(useful)selectedFeatures.add(feature);else droppedFeatures.add(feature);
        }
        features=selectedFeatures;
        if(features.isEmpty())throw new IllegalArgumentException("Feature selection removed every constant feature");
        List<PipelineStage> preprocessing=new ArrayList<>();
        List<String> assemblerInputs=new ArrayList<>(), indexedColumns=new ArrayList<>(), encodedColumns=new ArrayList<>();
        for(String feature:features){
            String type=raw.schema().apply(feature).dataType().typeName().toLowerCase(Locale.ROOT);
            if(numericTypes.contains(type)) assemblerInputs.add(feature);
            else {
                String indexed=feature+"__sbase_index", encoded=feature+"__sbase_ohe";
                preprocessing.add(new StringIndexer().setInputCol(feature).setOutputCol(indexed).setHandleInvalid("keep"));
                indexedColumns.add(indexed); encodedColumns.add(encoded); assemblerInputs.add(encoded);
            }
        }
        if(!indexedColumns.isEmpty()) preprocessing.add(new OneHotEncoder().setInputCols(indexedColumns.toArray(String[]::new))
                .setOutputCols(encodedColumns.toArray(String[]::new)).setHandleInvalid("keep"));
        VectorAssembler assembler=new VectorAssembler().setInputCols(assemblerInputs.toArray(String[]::new))
                .setOutputCol("features").setHandleInvalid("skip");
        preprocessing.add(assembler);
        long seed=((Number)request.getOrDefault("seed",42)).longValue();
        double validationFraction=((Number)request.getOrDefault("validationFraction",0.2)).doubleValue();
        if(validationFraction<0.1||validationFraction>0.4)throw new IllegalArgumentException("validationFraction must be between 0.1 and 0.4");
        Dataset<Row> train,validation;
        if(timeSeries){
            Dataset<Row> ordered=raw.orderBy(org.apache.spark.sql.functions.col(String.valueOf(request.get("timeColumn"))));
            int trainingCount=Math.max(2,(int)Math.floor(ordered.count()*(1.0-validationFraction)));
            train=ordered.limit(trainingCount);
            validation=ordered.exceptAll(train);
        }else{
            Dataset<Row>[] split=raw.randomSplit(new double[]{1.0-validationFraction,validationFraction},seed);
            train=split[0]; validation=split[1];
        }
        if(train.limit(2).count()<2||validation.limit(2).count()<2)throw new IllegalArgumentException("Not enough rows after validation split");
        List<PipelineStage> candidates=new ArrayList<>();
        if(task.equals("classification")){
            candidates.add(new LogisticRegression().setLabelCol(label).setFeaturesCol("features").setMaxIter(100).setRegParam(0.01));
            candidates.add(new RandomForestClassifier().setLabelCol(label).setFeaturesCol("features").setNumTrees(100).setMaxDepth(8).setSeed(seed));
            candidates.add(new DecisionTreeClassifier().setLabelCol(label).setFeaturesCol("features").setMaxDepth(8).setSeed(42));
        }else{
            candidates.add(new LinearRegression().setLabelCol(label).setFeaturesCol("features").setMaxIter(100).setRegParam(0.01));
            candidates.add(new RandomForestRegressor().setLabelCol(label).setFeaturesCol("features").setNumTrees(100).setMaxDepth(8).setSeed(42));
            candidates.add(new DecisionTreeRegressor().setLabelCol(label).setFeaturesCol("features").setMaxDepth(8).setSeed(42));
            if(timeSeries)candidates.add(new GBTRegressor().setLabelCol(label).setFeaturesCol("features").setMaxIter(80).setMaxDepth(5).setSeed(42));
        }
        int max=Math.max(1,Math.min(4,((Number)request.getOrDefault("maxCandidates",timeSeries?4:3)).intValue()));
        boolean classification=task.equals("classification");
        String metric=String.valueOf(request.getOrDefault("optimizationMetric",classification?"f1":"rmse")).toLowerCase(Locale.ROOT);
        Set<String> allowedMetrics=classification?Set.of("accuracy","f1","weightedprecision","weightedrecall"):Set.of("rmse","mse","mae","r2");
        if(!allowedMetrics.contains(metric))throw new IllegalArgumentException("Unsupported optimizationMetric: "+metric);
        boolean maximize=classification||metric.equals("r2");
        double bestScore=maximize?Double.NEGATIVE_INFINITY:Double.POSITIVE_INFINITY;
        PipelineModel best=null;String bestAlgorithm="";
        List<Map<String,Object>> trials=new ArrayList<>();
        for(PipelineStage estimator:candidates.stream().limit(max).toList()){
            String algorithm=estimator.getClass().getSimpleName();long started=System.nanoTime();
            try{
                List<PipelineStage> stages=new ArrayList<>(preprocessing);stages.add(estimator);
                Pipeline pipeline=new Pipeline().setStages(stages.toArray(PipelineStage[]::new));
                int folds=Math.max(2,Math.min(5,((Number)request.getOrDefault("cvFolds",3)).intValue()));
                CrossValidator validator=new CrossValidator().setEstimator(pipeline)
                        .setEvaluator(buildEvaluator(classification,label,metric))
                        .setEstimatorParamMaps(parameterGrid(estimator)).setNumFolds(folds)
                        .setParallelism(Math.max(1,Math.min(4,((Number)request.getOrDefault("cvParallelism",2)).intValue())))
                        .setSeed(seed);
                CrossValidatorModel cvModel=validator.fit(train);
                PipelineModel model=(PipelineModel)cvModel.bestModel();
                Dataset<Row> predictions=model.transform(validation);
                double score=buildEvaluator(classification,label,metric).evaluate(predictions);
                trials.add(Map.of("algorithm",algorithm,"metric",metric,"score",score,"cvFolds",folds,
                        "cvAverageMetric",Arrays.stream(cvModel.avgMetrics()).average().orElse(score),
                        "status","SUCCEEDED","durationMs",TimeUnitNanos.toMillis(System.nanoTime()-started)));
                if(maximize?score>bestScore:score<bestScore){best=model;bestScore=score;bestAlgorithm=algorithm;}
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
        Dataset<Row> bestPredictions=best.transform(validation);
        Map<String,Object> evaluation=classification?classificationMetrics(bestPredictions,label):regressionMetrics(bestPredictions,label);
        Map<String,Object> result=new LinkedHashMap<>();
        result.put("status","SUCCEEDED");result.put("task",task);result.put("algorithm",bestAlgorithm);
        result.put("metric",metric);result.put("score",bestScore);result.put("evaluationMetrics",evaluation);
        result.put("trainingRows",train.count());result.put("validationRows",validation.count());
        result.put("features",features);result.put("droppedConstantFeatures",droppedFeatures);
        result.put("categoricalEncoding","StringIndexer + OneHotEncoder");
        result.put("featureSelection","variance threshold for numeric and cardinality filter for categorical columns");
        result.put("timeSeriesLagFeatures",timeSeries);result.put("trials",trials);
        result.put("reproducibility",Map.of("seed",seed,"validationFraction",validationFraction,
                "validationStrategy",timeSeries?"chronological_holdout":"seeded_random_holdout","cvFolds",Math.max(2,Math.min(5,((Number)request.getOrDefault("cvFolds",3)).intValue()))));
        result.put("modelFormat","Spark PipelineModel");if(modelUri!=null)result.put("modelUri",modelUri);
        return result;
    }

    private static Evaluator buildEvaluator(boolean classification,String label,String metric) {
        return classification
                ?new MulticlassClassificationEvaluator().setLabelCol(label).setPredictionCol("prediction").setMetricName(metric)
                :new RegressionEvaluator().setLabelCol(label).setPredictionCol("prediction").setMetricName(metric);
    }

    private static Map<String,Object> classificationMetrics(Dataset<Row> predictions,String label) {
        Map<String,Object> metrics=new LinkedHashMap<>();
        MulticlassClassificationEvaluator evaluator=new MulticlassClassificationEvaluator().setLabelCol(label).setPredictionCol("prediction");
        metrics.put("accuracy",evaluator.setMetricName("accuracy").evaluate(predictions));
        metrics.put("weightedPrecision",evaluator.setMetricName("weightedPrecision").evaluate(predictions));
        metrics.put("weightedRecall",evaluator.setMetricName("weightedRecall").evaluate(predictions));
        metrics.put("weightedF1",evaluator.setMetricName("f1").evaluate(predictions));
        List<Map<String,Object>> confusion=new ArrayList<>();
        for(Row row:predictions.groupBy(org.apache.spark.sql.functions.col(label),org.apache.spark.sql.functions.col("prediction")).count().collectAsList()) {
            Map<String,Object> cell=new LinkedHashMap<>();
            cell.put("actual",row.isNullAt(0)?null:row.get(0));cell.put("predicted",row.isNullAt(1)?null:row.get(1));cell.put("count",row.getLong(2));
            confusion.add(cell);
        }
        metrics.put("confusionMatrix",confusion);
        List<Map<String,Object>> perClass=new ArrayList<>();
        List<Row> labels=predictions.select(org.apache.spark.sql.functions.col(label)).distinct().collectAsList();
        double recallSum=0.0;
        for(Row item:labels) {
            Object clazz=item.get(0);
            org.apache.spark.sql.Column actual=org.apache.spark.sql.functions.col(label);
            org.apache.spark.sql.Column predicted=org.apache.spark.sql.functions.col("prediction");
            long support=predictions.filter(actual.equalTo(org.apache.spark.sql.functions.lit(clazz))).count();
            long predictedCount=predictions.filter(predicted.equalTo(org.apache.spark.sql.functions.lit(clazz))).count();
            long truePositive=predictions.filter(actual.equalTo(org.apache.spark.sql.functions.lit(clazz))
                    .and(predicted.equalTo(org.apache.spark.sql.functions.lit(clazz)))).count();
            double precision=predictedCount==0?0.0:(double)truePositive/predictedCount;
            double recall=support==0?0.0:(double)truePositive/support;
            double f1=precision+recall==0?0.0:2.0*precision*recall/(precision+recall);
            recallSum+=recall;
            Map<String,Object> itemMetrics=new LinkedHashMap<>();
            itemMetrics.put("class",clazz);itemMetrics.put("support",support);itemMetrics.put("precision",precision);itemMetrics.put("recall",recall);itemMetrics.put("f1",f1);
            perClass.add(itemMetrics);
        }
        metrics.put("perClass",perClass);
        metrics.put("balancedAccuracy",labels.isEmpty()?0.0:recallSum/labels.size());
        if(labels.size()==2) {
            try {
                double auc=new BinaryClassificationEvaluator().setLabelCol(label).setRawPredictionCol("rawPrediction")
                        .setMetricName("areaUnderROC").evaluate(predictions);
                metrics.put("areaUnderROC",auc);
                double pr=new BinaryClassificationEvaluator().setLabelCol(label).setRawPredictionCol("rawPrediction")
                        .setMetricName("areaUnderPR").evaluate(predictions);
                metrics.put("areaUnderPR",pr);
            } catch(Exception ignored) { metrics.put("aucUnavailable","Classifier did not produce compatible rawPrediction scores"); }
        }
        return metrics;
    }

    private static Map<String,Object> regressionMetrics(Dataset<Row> predictions,String label) {
        Map<String,Object> metrics=new LinkedHashMap<>();
        RegressionEvaluator evaluator=new RegressionEvaluator().setLabelCol(label).setPredictionCol("prediction");
        metrics.put("rmse",evaluator.setMetricName("rmse").evaluate(predictions));
        metrics.put("mse",evaluator.setMetricName("mse").evaluate(predictions));
        metrics.put("mae",evaluator.setMetricName("mae").evaluate(predictions));
        metrics.put("r2",evaluator.setMetricName("r2").evaluate(predictions));
        org.apache.spark.sql.Column actual=org.apache.spark.sql.functions.col(label);
        org.apache.spark.sql.Column nonZero=actual.notEqual(0);
        Row mape=predictions.filter(nonZero).select(org.apache.spark.sql.functions.avg(
                org.apache.spark.sql.functions.abs(org.apache.spark.sql.functions.col("prediction").minus(actual).divide(actual)))).first();
        metrics.put("mape",mape.isNullAt(0)?null:mape.getDouble(0)*100.0);
        return metrics;
    }

    private static org.apache.spark.ml.param.ParamMap[] parameterGrid(PipelineStage estimator) {
        ParamGridBuilder grid=new ParamGridBuilder();
        if(estimator instanceof LogisticRegression model)
            grid.addGrid(model.regParam(),new double[]{0.001,0.01,0.1})
                .addGrid(model.elasticNetParam(),new double[]{0.0,0.5});
        else if(estimator instanceof LinearRegression model)
            grid.addGrid(model.regParam(),new double[]{0.001,0.01,0.1});
        else if(estimator instanceof RandomForestClassifier model)
            grid.addGrid(model.numTrees(),new int[]{50,100}).addGrid(model.maxDepth(),new int[]{5,8});
        else if(estimator instanceof RandomForestRegressor model)
            grid.addGrid(model.numTrees(),new int[]{50,100}).addGrid(model.maxDepth(),new int[]{5,8});
        else if(estimator instanceof DecisionTreeClassifier model)
            grid.addGrid(model.maxDepth(),new int[]{4,8,12});
        else if(estimator instanceof DecisionTreeRegressor model)
            grid.addGrid(model.maxDepth(),new int[]{4,8,12});
        else if(estimator instanceof GBTRegressor model)
            grid.addGrid(model.maxIter(),new int[]{40,80}).addGrid(model.maxDepth(),new int[]{3,5});
        return grid.build();
    }
    private static final class TimeUnitNanos{
        static long toMillis(long nanos){return java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(nanos);}
    }
}
