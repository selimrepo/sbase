package com.sbase.platform.ml;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.*;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.*;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.regression.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executor;
import static org.apache.spark.sql.functions.*;

@Service
public class MlLifecycleService {
 private final JdbcTemplate db; private final ObjectMapper json; private final ObjectProvider<SparkSession> spark;
 private final Executor executor; private final String jdbcUrl,user,password,artifactRoot; private final Set<String> tables;
 private final java.util.concurrent.ConcurrentHashMap<String,PipelineModel> loadedModels=new java.util.concurrent.ConcurrentHashMap<>();
 public MlLifecycleService(@Qualifier("jdbcTemplate") JdbcTemplate db,ObjectMapper json,ObjectProvider<SparkSession> spark,
   @Qualifier("miningExecutor") Executor executor,@Value("${spring.datasource.url}") String jdbcUrl,
   @Value("${spring.datasource.username}") String user,@Value("${spring.datasource.password}") String password,
   @Value("${sbase.ml.artifact-root:./data/ml-artifacts}") String artifactRoot,
   @Value("${sbase.ml.allowed-tables:}") String allowedTables) {
  this.db=db;this.json=json;this.spark=spark;this.executor=executor;this.jdbcUrl=jdbcUrl;this.user=user;this.password=password;this.artifactRoot=artifactRoot;
  this.tables=new HashSet<>();for(String t:allowedTables.split(","))if(!t.isBlank())this.tables.add(t.trim().toLowerCase(Locale.ROOT));
 }
 public UUID createExperiment(UUID tenant,String name,String description,String owner) {
  UUID id=UUID.randomUUID();db.update("INSERT INTO sbase_ml_experiments(id,tenant_id,name,description,owner_subject) VALUES(?,?,?,?,?)",id,tenant,name,description,owner);return id;
 }
 public List<Map<String,Object>> experiments(UUID tenant,String subject) {
  return db.queryForList("SELECT * FROM sbase_ml_experiments WHERE tenant_id=? AND owner_subject=? ORDER BY created_at DESC LIMIT 200",tenant,subject);
 }
 public UUID submit(UUID tenant,UUID experiment,String runName,String algorithm,String task,String table,List<String> features,String label,
                    int folds,JsonNode parameters,JsonNode searchSpace,String subject) {
  validateTable(table);if(features==null||features.isEmpty()||features.size()>100)throw new IllegalArgumentException("Provide 1..100 feature columns");
  if(label==null||!label.matches("[A-Za-z_][A-Za-z0-9_]{0,127}"))throw new IllegalArgumentException("Invalid label column");
  features.forEach(MlLifecycleService::identifier);
  if(folds<1||folds>10)throw new IllegalArgumentException("Cross-validation folds must be 1..10");
  if(!Set.of("CLASSIFICATION","REGRESSION").contains(task))throw new IllegalArgumentException("Unsupported ML task");
  if(!Set.of("LOGISTIC_REGRESSION","RANDOM_FOREST_CLASSIFIER","LINEAR_REGRESSION","RANDOM_FOREST_REGRESSOR").contains(algorithm))throw new IllegalArgumentException("Unsupported algorithm");
  if(task.equals("CLASSIFICATION")&&!Set.of("LOGISTIC_REGRESSION","RANDOM_FOREST_CLASSIFIER").contains(algorithm))throw new IllegalArgumentException("Algorithm does not match classification task");
  if(task.equals("REGRESSION")&&!Set.of("LINEAR_REGRESSION","RANDOM_FOREST_REGRESSOR").contains(algorithm))throw new IllegalArgumentException("Algorithm does not match regression task");
  Integer owned=db.query("SELECT count(*) FROM sbase_ml_experiments WHERE id=? AND tenant_id=? AND owner_subject=?",
    rs->rs.next()?rs.getInt(1):0,experiment,tenant,subject);
  if(owned==null||owned==0)throw new org.springframework.security.access.AccessDeniedException("Experiment not found or not owned by user");
  UUID id=UUID.randomUUID();
  db.update("""
   INSERT INTO sbase_ml_runs(id,tenant_id,experiment_id,run_name,algorithm,task_type,status,parameters_json,search_space_json,
   dataset_table,feature_columns_json,label_column,cross_validation_folds,submitted_by)
   VALUES(?,?,?,?,?,?,'QUEUED',CAST(? AS jsonb),CAST(? AS jsonb),?,CAST(? AS jsonb),?,?,?)
   """,id,tenant,experiment,runName,algorithm,task,parameters==null?"{}":parameters.toString(),
    searchSpace==null?"{}":searchSpace.toString(),table,featuresJson(features),label,folds,subject);
  try{executor.execute(()->train(id,tenant,table,features,label,algorithm,task,folds,parameters,searchSpace));}catch(RuntimeException ex){db.update("UPDATE sbase_ml_runs SET status='FAILED',error_message=?,finished_at=now() WHERE id=? AND tenant_id=?",ex.getMessage(),id,tenant);throw ex;}
  return id;
 }
 private void train(UUID id,UUID tenant,String table,List<String> features,String label,String algorithm,String task,int folds,JsonNode parameters,JsonNode search) {
  if (db.update("UPDATE sbase_ml_runs SET status='RUNNING',started_at=now() WHERE id=? AND tenant_id=? AND status='QUEUED'",id,tenant) != 1) return;
  try {
   SparkSession session=spark.getObject();
   Properties props=new Properties();props.setProperty("user",user);props.setProperty("password",password);props.setProperty("driver","org.postgresql.Driver");
   String projection=String.join(",",features)+","+label;
   Dataset<Row> data=session.read().jdbc(jdbcUrl,"(SELECT "+projection+" FROM "+table+" WHERE tenant_id='"+tenant+"' AND "+label+" IS NOT NULL) s",props)
     .na().drop(new String[]{label});
   if(data.limit(2).count()<2)throw new IllegalArgumentException("Training dataset must contain at least two labeled rows");
   VectorAssembler assembler=new VectorAssembler().setInputCols(features.toArray(String[]::new)).setOutputCol("features").setHandleInvalid("skip");
   PipelineStage estimator=estimator(algorithm,label,parameters);
   Pipeline pipeline=new Pipeline().setStages(new PipelineStage[]{assembler,estimator});
   Dataset<Row>[] split=data.randomSplit(new double[]{0.8,0.2},42L);
   Dataset<Row> train=split[0],test=split[1];
   if(train.limit(2).count()<2||test.limit(1).count()<1) {train=data;test=data;}
   PipelineModel fitted;
   Map<String,Object> best=new LinkedHashMap<>();
   if(folds>1) {
    CrossValidator cv=new CrossValidator().setEstimator(pipeline).setEvaluator(evaluator(task,label)).setNumFolds(folds).setParallelism(1).setSeed(42L);
    ParamMap[] grid=parameterGrid(algorithm,search,estimator);
    cv.setEstimatorParamMaps(grid);
    CrossValidatorModel cvModel=cv.fit(train);
    PipelineModel bestPipeline=(PipelineModel)cvModel.bestModel();fitted=bestPipeline;
    best.put("crossValidationMetrics",Arrays.stream(cvModel.avgMetrics()).boxed().toList());
    best.put("bestAverageMetric",task.equals("CLASSIFICATION")?Arrays.stream(cvModel.avgMetrics()).max().orElse(0):Arrays.stream(cvModel.avgMetrics()).min().orElse(0));
    best.put("bestEstimatorParameters",bestPipeline.stages()[1].extractParamMap().toString());
   } else fitted=pipeline.fit(train);
   double metric=evaluator(task,label).evaluate(fitted.transform(test));
   String metricName=task.equals("CLASSIFICATION")?"accuracy":"rmse";
   Path target=Paths.get(artifactRoot).resolve(tenant.toString()).resolve(id.toString()).toAbsolutePath();
   Files.createDirectories(target.getParent());fitted.write().overwrite().save(target.toString());
   Map<String,Object> metrics=new LinkedHashMap<>();metrics.put(metricName,metric);metrics.put("trainingRows",train.count());metrics.put("validationRows",test.count());metrics.put("crossValidationFolds",folds);
   Map<String,Object> baseline=featureBaseline(data,features);
   db.update("UPDATE sbase_ml_runs SET status='SUCCEEDED',metrics_json=CAST(? AS jsonb),best_parameters_json=CAST(? AS jsonb),feature_baseline_json=CAST(? AS jsonb),artifact_uri=?,finished_at=now() WHERE id=? AND tenant_id=?",
    json.writeValueAsString(metrics),json.writeValueAsString(best),json.writeValueAsString(baseline),target.toUri().toString(),id,tenant);
  } catch(Exception ex) {
   String msg=ex.getMessage()==null?ex.getClass().getSimpleName():ex.getMessage();
   db.update("UPDATE sbase_ml_runs SET status='FAILED',error_message=?,finished_at=now() WHERE id=? AND tenant_id=?",msg.substring(0,Math.min(msg.length(),4000)),id,tenant);
  }
 }
 private Map<String,Object> featureBaseline(Dataset<Row> data,List<String> features) {
  List<Column> expressions=new ArrayList<>();for(String feature:features){expressions.add(avg(col(feature)).alias(feature+"__mean"));expressions.add(stddev(col(feature)).alias(feature+"__stddev"));}
  Column[] all=expressions.toArray(Column[]::new);Row row=data.agg(all[0],Arrays.copyOfRange(all,1,all.length)).first();
  Map<String,Object> result=new TreeMap<>();for(int i=0;i<features.size();i++){Object mean=row.get(i*2),std=row.get(i*2+1);result.put(features.get(i),Map.of("mean",mean instanceof Number n?n.doubleValue():0.0,"stddev",std instanceof Number n?n.doubleValue():0.0));}return result;
 }
 private PipelineStage estimator(String algorithm,String label,JsonNode p) {
  return switch(algorithm) {
   case "LOGISTIC_REGRESSION" -> new LogisticRegression().setLabelCol(label).setFeaturesCol("features").setMaxIter(intParam(p,"maxIter",50)).setRegParam(doubleParam(p,"regParam",0.0));
   case "RANDOM_FOREST_CLASSIFIER" -> new RandomForestClassifier().setLabelCol(label).setFeaturesCol("features").setNumTrees(intParam(p,"numTrees",50)).setMaxDepth(intParam(p,"maxDepth",5));
   case "LINEAR_REGRESSION" -> new LinearRegression().setLabelCol(label).setFeaturesCol("features").setMaxIter(intParam(p,"maxIter",50)).setRegParam(doubleParam(p,"regParam",0.0));
   case "RANDOM_FOREST_REGRESSOR" -> new RandomForestRegressor().setLabelCol(label).setFeaturesCol("features").setNumTrees(intParam(p,"numTrees",50)).setMaxDepth(intParam(p,"maxDepth",5));
   default -> throw new IllegalArgumentException("Unsupported estimator");
  };
 }
 private ParamMap[] parameterGrid(String algorithm,JsonNode s,PipelineStage stage) {
  if(s!=null&&!s.isObject())throw new IllegalArgumentException("Hyperparameter search space must be a JSON object");
  Set<String> supported=Set.of("RANDOM_FOREST_CLASSIFIER","RANDOM_FOREST_REGRESSOR").contains(algorithm)
   ?Set.of("numTrees","maxDepth"):Set.of("regParam","maxIter");
  if(s!=null){Iterator<String> names=s.fieldNames();while(names.hasNext()){String name=names.next();if(!supported.contains(name))throw new IllegalArgumentException("Hyperparameter "+name+" is not supported by "+algorithm);}}
  ParamGridBuilder b=new ParamGridBuilder();boolean added=false;
  if(s!=null&&s.isObject()) {
   JsonNode n=s.path("numTrees");if(n.isArray()){int[] v=ints(n,1,500);if(stage instanceof RandomForestClassifier m){b.addGrid(m.numTrees(),v);added=true;}if(stage instanceof RandomForestRegressor m){b.addGrid(m.numTrees(),v);added=true;}}
   n=s.path("maxDepth");if(n.isArray()){int[] v=ints(n,1,30);if(stage instanceof RandomForestClassifier m){b.addGrid(m.maxDepth(),v);added=true;}if(stage instanceof RandomForestRegressor m){b.addGrid(m.maxDepth(),v);added=true;}}
   n=s.path("regParam");if(n.isArray()){double[] v=doubles(n);if(stage instanceof LogisticRegression m){b.addGrid(m.regParam(),v);added=true;}if(stage instanceof LinearRegression m){b.addGrid(m.regParam(),v);added=true;}}
   n=s.path("maxIter");if(n.isArray()){int[] v=ints(n,1,500);if(stage instanceof LogisticRegression m){b.addGrid(m.maxIter(),v);added=true;}if(stage instanceof LinearRegression m){b.addGrid(m.maxIter(),v);added=true;}}
  }
  ParamMap[] grid=added?b.build():new ParamGridBuilder().build();if(grid.length>64)throw new IllegalArgumentException("Hyperparameter grid expands to more than 64 combinations");return grid;
 }
 private Evaluator evaluator(String task,String label){return task.equals("CLASSIFICATION")?new MulticlassClassificationEvaluator().setLabelCol(label).setPredictionCol("prediction").setMetricName("accuracy"):new RegressionEvaluator().setLabelCol(label).setPredictionCol("prediction").setMetricName("rmse");}
 @org.springframework.transaction.annotation.Transactional
 public UUID registerModel(UUID tenant,UUID run,String name,String description,String subject) {
  Map<String,Object> r=db.queryForMap("SELECT artifact_uri,algorithm,task_type,metrics_json,feature_columns_json,label_column,feature_baseline_json FROM sbase_ml_runs WHERE id=? AND tenant_id=? AND status='SUCCEEDED'",run,tenant);
  UUID model=db.query("SELECT id FROM sbase_ml_models WHERE tenant_id=? AND name=?",(rs)->rs.next()?rs.getObject(1,UUID.class):null,tenant,name);
  if(model==null){model=UUID.randomUUID();db.update("INSERT INTO sbase_ml_models(id,tenant_id,name,description,owner_subject) VALUES(?,?,?,?,?)",model,tenant,name,description,subject);}
  Integer version=db.queryForObject("SELECT COALESCE(MAX(version),0)+1 FROM sbase_ml_model_versions WHERE model_id=? AND tenant_id=?",Integer.class,model,tenant);
  db.update("INSERT INTO sbase_ml_model_versions(id,tenant_id,model_id,run_id,version,artifact_uri,framework,algorithm,task_type,metrics_json,signature_json,feature_baseline_json,created_by) VALUES(?,?,?,?,?,?,'Spark MLlib',?,?,CAST(? AS jsonb),CAST(? AS jsonb),CAST(? AS jsonb),?)",
    UUID.randomUUID(),tenant,model,run,version,r.get("artifact_uri"),r.get("algorithm"),r.get("task_type"),r.get("metrics_json").toString(),signatureJson(r.get("feature_columns_json").toString(),r.get("label_column")),r.get("feature_baseline_json").toString(),subject);
  return model;
 }
 public List<Map<String,Object>> compare(UUID tenant,UUID experiment) {
  return db.queryForList("SELECT id,run_name,algorithm,task_type,status,parameters_json,metrics_json,best_parameters_json,created_at FROM sbase_ml_runs WHERE tenant_id=? AND experiment_id=? ORDER BY created_at DESC",tenant,experiment);
 }
 public List<Map<String,Object>> models(UUID tenant) {return db.queryForList("SELECT m.id,m.name,m.description,v.id AS version_id,v.version,v.stage,v.algorithm,v.task_type,v.metrics_json,v.artifact_uri FROM sbase_ml_models m JOIN sbase_ml_model_versions v ON v.model_id=m.id WHERE m.tenant_id=? ORDER BY m.created_at DESC,v.version DESC",tenant);}
 public UUID deploy(UUID tenant,UUID version,String name,String subject) {
  Integer ok=db.queryForObject("SELECT count(*) FROM sbase_ml_model_versions WHERE id=? AND tenant_id=?",Integer.class,version,tenant);
  if(ok==null||ok==0)throw new IllegalArgumentException("Model version not found");
  UUID id=UUID.randomUUID();db.update("INSERT INTO sbase_ml_deployments(id,tenant_id,model_version_id,deployment_name,status,created_by) VALUES(?,?,?,?,'ACTIVE',?)",id,tenant,version,name,subject);return id;
 }
 public Map<String,Object> predict(UUID tenant,UUID deployment,Map<String,Object> input) {
  long start=System.nanoTime();boolean ok=false,found=false;Map<String,Object> result=new LinkedHashMap<>();
  try {
   Map<String,Object> m=db.queryForMap("SELECT v.artifact_uri,v.signature_json,v.feature_baseline_json,d.id FROM sbase_ml_deployments d JOIN sbase_ml_model_versions v ON v.id=d.model_version_id WHERE d.id=? AND d.tenant_id=? AND d.status='ACTIVE'",deployment,tenant);found=true;
   @SuppressWarnings("unchecked") Map<String,Object> signature=json.readValue(m.get("signature_json").toString(),Map.class);
   List<String> cols=(List<String>)signature.getOrDefault("features",List.of());
   if(cols.isEmpty())throw new IllegalStateException("Model signature is missing feature columns");
   for(String c:cols)if(!input.containsKey(c))throw new IllegalArgumentException("Missing feature: "+c);
   StructField[] fields=cols.stream().map(c->DataTypes.createStructField(c,DataTypes.DoubleType,true)).toArray(StructField[]::new);
   StructType schema=DataTypes.createStructType(fields);
   Row row=RowFactory.create(cols.stream().map(c->((Number)input.get(c)).doubleValue()).toArray());
   Dataset<Row> frame=spark.getObject().createDataFrame(List.of(row),schema);
   String artifact=(String)m.get("artifact_uri");
   Path artifactPath=tenantArtifactPath(tenant,artifact);
   String cacheKey=tenant+":"+artifactPath;
   PipelineModel model=loadedModels.computeIfAbsent(cacheKey,key->PipelineModel.load(artifactPath.toString()));
   Row prediction=model.transform(frame).select("prediction").first();
   result.put("prediction",prediction.get(0));ok=true;return result;
  } catch(Exception ex){result.put("error",ex.getMessage());throw new IllegalStateException("Model prediction failed",ex);}
  finally {
   long latency=(System.nanoTime()-start)/1_000_000;
   if(found)db.update("INSERT INTO sbase_ml_prediction_events(tenant_id,deployment_id,latency_ms,succeeded,prediction_json,error_code,input_json) VALUES(?,?,?,?,CAST(? AS jsonb),?,CAST(? AS jsonb))",
    tenant,deployment,latency,ok,ok?toJson(result):"{}",ok?null:"PREDICTION_FAILED",toJson(input));
  }
 }
 public Map<String,Object> monitor(UUID tenant,UUID deployment) {
  Map<String,Object> metrics=db.queryForMap("SELECT count(*) AS requests,count(*) FILTER(WHERE succeeded) AS successes,count(*) FILTER(WHERE NOT succeeded) AS failures,COALESCE(avg(latency_ms),0) AS avg_latency_ms,COALESCE(percentile_cont(0.95) WITHIN GROUP(ORDER BY latency_ms),0) AS p95_latency_ms,MAX(created_at) AS last_prediction_at FROM sbase_ml_prediction_events WHERE tenant_id=? AND deployment_id=? AND created_at>=now()-interval '24 hours'",tenant,deployment);
  List<Map<String,Object>> raw=db.queryForList("SELECT input_json FROM sbase_ml_prediction_events WHERE tenant_id=? AND deployment_id=? AND succeeded=true ORDER BY created_at DESC LIMIT 200",tenant,deployment);
  metrics.put("recentInputSamples",raw.size());
  try {
   Map<String,Object> version=db.queryForMap("SELECT v.feature_baseline_json FROM sbase_ml_deployments d JOIN sbase_ml_model_versions v ON v.id=d.model_version_id WHERE d.id=? AND d.tenant_id=?",deployment,tenant);
   Map<String,Map<String,Number>> baseline=json.readValue(version.get("feature_baseline_json").toString(),Map.class);
   Map<String,List<Double>> observed=new TreeMap<>();
   for(Map<String,Object> event:raw){Object payload=event.get("input_json");Map<String,Object> values=payload instanceof Map<?,?> m?(Map<String,Object>)m:json.readValue(payload.toString(),Map.class);
    for(var entry:values.entrySet())if(entry.getValue() instanceof Number n)observed.computeIfAbsent(entry.getKey(),k->new ArrayList<>()).add(n.doubleValue());}
   Map<String,Object> drift=new TreeMap<>();List<String> drifted=new ArrayList<>();
   for(var entry:baseline.entrySet()){List<Double> samples=observed.get(entry.getKey());if(samples==null||samples.isEmpty())continue;double mean=samples.stream().mapToDouble(Double::doubleValue).average().orElse(0);double baseMean=entry.getValue().getOrDefault("mean",0).doubleValue();double std=Math.max(1e-9,entry.getValue().getOrDefault("stddev",0).doubleValue());double z=Math.abs(mean-baseMean)/std;drift.put(entry.getKey(),Map.of("trainingMean",baseMean,"servingMean",mean,"zScore",z,"samples",samples.size()));if(z>=3.0)drifted.add(entry.getKey());}
   metrics.put("featureDrift",drift);metrics.put("driftedFeatures",drifted);
  }catch(Exception ex){metrics.put("featureDriftAvailable",false);}
  return metrics;
 }
 @org.springframework.transaction.annotation.Transactional
 public void promote(UUID tenant,UUID version,String stage) {
  if(!Set.of("NONE","STAGING","PRODUCTION","ARCHIVED").contains(stage))throw new IllegalArgumentException("Invalid model stage");
  UUID model=db.query("SELECT model_id FROM sbase_ml_model_versions WHERE id=? AND tenant_id=?",(rs)->rs.next()?rs.getObject(1,UUID.class):null,version,tenant);
  if(model==null)throw new IllegalArgumentException("Model version not found");
  if("PRODUCTION".equals(stage))db.update("UPDATE sbase_ml_model_versions SET stage='ARCHIVED' WHERE tenant_id=? AND model_id=? AND stage='PRODUCTION'",tenant,model);
  db.update("UPDATE sbase_ml_model_versions SET stage=?,promoted_at=CASE WHEN ?='PRODUCTION' THEN now() ELSE promoted_at END WHERE id=? AND tenant_id=?",stage,stage,version,tenant);
 }
 public Map<String,Object> lineage(UUID tenant,UUID version) {
  return db.queryForMap("SELECT m.id AS model_id,m.name AS model_name,v.id AS version_id,v.version,v.stage,v.artifact_uri,v.run_id,r.experiment_id,r.run_name,r.algorithm,r.parameters_json,r.search_space_json,r.metrics_json,r.dataset_table,r.feature_columns_json,r.label_column,r.submitted_by,r.created_at AS run_created_at FROM sbase_ml_model_versions v JOIN sbase_ml_models m ON m.id=v.model_id LEFT JOIN sbase_ml_runs r ON r.id=v.run_id WHERE v.id=? AND v.tenant_id=? AND m.tenant_id=?",version,tenant,tenant);
 }
 public Map<String,Object> batchResult(UUID tenant,UUID id) {
  return db.queryForMap("SELECT id,status,row_count,result_json,error_message,created_at,finished_at FROM sbase_ml_batch_jobs WHERE id=? AND tenant_id=?",id,tenant);
 }
 public UUID batchPredict(UUID tenant,UUID deployment,List<Map<String,Object>> rows) {
  if(rows==null||rows.isEmpty()||rows.size()>10000)throw new IllegalArgumentException("Batch must contain 1..10000 records");
  UUID id=UUID.randomUUID();
  int created=db.update("INSERT INTO sbase_ml_batch_jobs(id,tenant_id,model_version_id,input_table,output_table,status,row_count,submitted_by) SELECT ?,tenant_id,model_version_id,'API_PAYLOAD','API_RESPONSE','QUEUED',0,'api' FROM sbase_ml_deployments WHERE id=? AND tenant_id=? AND status='ACTIVE'",id,deployment,tenant);
  if(created==0)throw new IllegalArgumentException("Active deployment not found");
  List<Map<String,Object>> payload=List.copyOf(rows);
  try{executor.execute(()->{
   db.update("UPDATE sbase_ml_batch_jobs SET status='RUNNING' WHERE id=? AND tenant_id=? AND status='QUEUED'",id,tenant);
   try{List<Map<String,Object>> predictions=new ArrayList<>();for(Map<String,Object> row:payload)predictions.add(predict(tenant,deployment,row));
    db.update("UPDATE sbase_ml_batch_jobs SET status='SUCCEEDED',row_count=?,result_json=CAST(? AS jsonb),finished_at=now() WHERE id=? AND tenant_id=?",predictions.size(),toJson(predictions),id,tenant);
   }catch(Exception ex){String message=ex.getMessage()==null?ex.getClass().getSimpleName():ex.getMessage();db.update("UPDATE sbase_ml_batch_jobs SET status='FAILED',error_message=?,finished_at=now() WHERE id=? AND tenant_id=?",message.substring(0,Math.min(4000,message.length())),id,tenant);}
  });}catch(RuntimeException ex){db.update("UPDATE sbase_ml_batch_jobs SET status='FAILED',error_message=?,finished_at=now() WHERE id=? AND tenant_id=?",ex.getMessage(),id,tenant);throw ex;}
  return id;
 }
 private String toJson(Object value){try{return json.writeValueAsString(value);}catch(Exception ex){return "{}";}}
 private String signatureJson(String featureJson,Object label){try{return json.writeValueAsString(Map.of("features",json.readValue(featureJson,List.class),"label",String.valueOf(label)));}catch(Exception ex){throw new IllegalStateException("Unable to build model signature",ex);}}
 private Path tenantArtifactPath(UUID tenant,String artifactUri) {
  if (tenant==null || artifactUri==null || artifactUri.isBlank())
   throw new org.springframework.security.access.AccessDeniedException("Model artifact is not available for this tenant");
  try {
   java.net.URI uri=java.net.URI.create(artifactUri);
   if (!"file".equalsIgnoreCase(uri.getScheme()))
    throw new org.springframework.security.access.AccessDeniedException("Unsupported model artifact location");
   Path root=Paths.get(artifactRoot).toAbsolutePath().normalize().resolve(tenant.toString()).normalize();
   Path artifact=Paths.get(uri).toAbsolutePath().normalize();
   if (!artifact.startsWith(root) || artifact.equals(root))
    throw new org.springframework.security.access.AccessDeniedException("Model artifact belongs to a different tenant");
   Path realRoot=root.toRealPath();
   Path realArtifact=artifact.toRealPath();
   if (!realArtifact.startsWith(realRoot) || realArtifact.equals(realRoot))
    throw new org.springframework.security.access.AccessDeniedException("Model artifact resolves outside its tenant directory");
   return realArtifact;
  } catch (org.springframework.security.access.AccessDeniedException ex) { throw ex; }
    catch (RuntimeException | java.io.IOException ex) {
     throw new org.springframework.security.access.AccessDeniedException("Invalid model artifact location");
    }
 }
 private void validateTable(String table){identifier(table);if(!tables.contains(table.toLowerCase(Locale.ROOT)))throw new IllegalArgumentException("Training table is not allow-listed");}
 private static void identifier(String s){if(s==null||!s.matches("[A-Za-z_][A-Za-z0-9_]{0,127}(\\.[A-Za-z_][A-Za-z0-9_]{0,127})?"))throw new IllegalArgumentException("Unsafe SQL identifier");}
 private String featuresJson(List<String> f){try{return json.writeValueAsString(f);}catch(Exception e){throw new IllegalArgumentException(e);}}
 private int intParam(JsonNode n,String key,int fallback){int v=n==null?fallback:n.path(key).asInt(fallback);if(v<1||v>500)throw new IllegalArgumentException("Invalid "+key);return v;}
 private double doubleParam(JsonNode n,String key,double fallback){double v=n==null?fallback:n.path(key).asDouble(fallback);if(!Double.isFinite(v)||v<0||v>1000)throw new IllegalArgumentException("Invalid "+key);return v;}
 private int[] ints(JsonNode n,int min,int max){if(n.isEmpty())throw new IllegalArgumentException("Hyperparameter candidate arrays cannot be empty");if(n.size()>8)throw new IllegalArgumentException("At most 8 tuning candidates per parameter");int[] a=new int[n.size()];for(int i=0;i<a.length;i++){a[i]=n.get(i).asInt();if(a[i]<min||a[i]>max)throw new IllegalArgumentException("Tuning parameter outside supported range");}return a;}
 private double[] doubles(JsonNode n){if(n.isEmpty())throw new IllegalArgumentException("Hyperparameter candidate arrays cannot be empty");if(n.size()>8)throw new IllegalArgumentException("At most 8 tuning candidates per parameter");double[] a=new double[n.size()];for(int i=0;i<a.length;i++){a[i]=n.get(i).asDouble();if(!Double.isFinite(a[i])||a[i]<0||a[i]>1000)throw new IllegalArgumentException("Invalid tuning parameter");}return a;}
}
