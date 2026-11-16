package io.sbase.server.automl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.*;
import java.nio.file.*;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/** Durable experiment lifecycle and versioned Spark PipelineModel registry backed by PostgreSQL. */
public final class AutoMlLifecycleService {
    private static final ObjectMapper JSON=new ObjectMapper();
    private final SparkSession spark;
    private final String jdbcUrl=env("SBASE_POSTGRES_URL","jdbc:postgresql://localhost:5432/sbase");
    private final String user=env("SBASE_POSTGRES_USER","sbase");
    private final String password=env("SBASE_POSTGRES_PASSWORD","");
    private final Path registryRoot=Path.of(env("SBASE_MODEL_REGISTRY_PATH","/var/lib/sbase/models")).toAbsolutePath().normalize();

    public AutoMlLifecycleService(SparkSession spark){this.spark=Objects.requireNonNull(spark);}

    public Map<String,Object> train(Map<String,Object> request) throws Exception {
        UUID experimentId=UUID.randomUUID();
        String name=String.valueOf(request.getOrDefault("name","AutoML-"+experimentId.toString().substring(0,8)));
        String task=String.valueOf(request.getOrDefault("task","classification")).toUpperCase(Locale.ROOT);
        String label=String.valueOf(request.getOrDefault("label","label"));
        List<?> features=(List<?>)request.getOrDefault("features",List.of());
        if(name.length()>200)throw new IllegalArgumentException("Experiment name is limited to 200 characters");
        if(!Set.of("CLASSIFICATION","REGRESSION").contains(task))throw new IllegalArgumentException("Unsupported AutoML task");
        String modelPath=registryRoot.resolve(experimentId.toString()).toString();
        insertExperiment(experimentId,name,task,label,JSON.writeValueAsString(features));
        updateStatus(experimentId,"RUNNING",null,null,null,null);
        long started=System.nanoTime();
        try{
            Map<String,Object> result=new AutoMlService(spark).train(request,modelPath);
            String algorithm=String.valueOf(result.get("algorithm"));
            String metric=String.valueOf(result.get("metric"));
            double score=((Number)result.get("score")).doubleValue();
            List<Map<String,Object>> trials=(List<Map<String,Object>>)result.get("trials");
            persistTrials(experimentId,trials);
            UUID modelId=UUID.randomUUID();
            registerModel(modelId,experimentId,name,algorithm,task,metric,score,modelPath,features);
            updateStatus(experimentId,"SUCCEEDED",algorithm,metric,score,modelPath);
            Map<String,Object> response=new LinkedHashMap<>(result);
            response.put("experimentId",experimentId.toString());
            response.put("modelId",modelId.toString());
            response.put("modelUri",modelPath);
            response.put("durationMs",java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-started));
            return response;
        }catch(Exception error){
            updateStatus(experimentId,"FAILED",null,null,null,null);
            throw error;
        }
    }

    public List<Map<String,Object>> experiments(int limit)throws SQLException{
        String sql="SELECT id,name,task_type,label_column,status,best_algorithm,best_metric,best_score,model_uri,created_at,finished_at FROM automl_experiment ORDER BY created_at DESC LIMIT ?";
        try(Connection c=connection();PreparedStatement p=c.prepareStatement(sql)){
            p.setInt(1,Math.max(1,Math.min(limit,200)));
            try(ResultSet r=p.executeQuery()){List<Map<String,Object>> rows=new ArrayList<>();while(r.next()){
                Map<String,Object> row=new LinkedHashMap<>();row.put("id",r.getObject("id").toString());row.put("name",r.getString("name"));
                row.put("task",r.getString("task_type"));row.put("label",r.getString("label_column"));row.put("status",r.getString("status"));
                row.put("algorithm",r.getString("best_algorithm"));row.put("metric",r.getString("best_metric"));row.put("score",r.getObject("best_score"));
                row.put("modelUri",r.getString("model_uri"));row.put("createdAt",r.getTimestamp("created_at").toInstant().toString());
                Timestamp finished=r.getTimestamp("finished_at");row.put("finishedAt",finished==null?null:finished.toInstant().toString());rows.add(row);
            }return rows;}
        }
    }

    public Map<String,Object> model(UUID id)throws SQLException{
        try(Connection c=connection();PreparedStatement p=c.prepareStatement("SELECT id,experiment_id,name,algorithm,task_type,metric,best_score,model_uri,version,status,created_at FROM model_registry WHERE id=?")){
            p.setObject(1,id);try(ResultSet r=p.executeQuery()){if(!r.next())throw new NoSuchElementException("Model not found");
                Map<String,Object> m=new LinkedHashMap<>();m.put("id",r.getObject("id").toString());m.put("experimentId",r.getObject("experiment_id").toString());
                m.put("name",r.getString("name"));m.put("algorithm",r.getString("algorithm"));m.put("task",r.getString("task_type"));
                m.put("metric",r.getString("metric"));m.put("score",r.getDouble("best_score"));m.put("modelUri",r.getString("model_uri"));
                m.put("version",r.getInt("version"));m.put("status",r.getString("status"));m.put("createdAt",r.getTimestamp("created_at").toInstant().toString());return m;}
        }
    }

    public Map<String,Object> predict(UUID modelId,List<Map<String,Object>> rows)throws Exception{
        if(rows==null||rows.isEmpty()||rows.size()>50000)throw new IllegalArgumentException("Prediction requires 1..50000 rows");
        Map<String,Object> metadata=model(modelId);
        String path=String.valueOf(metadata.get("modelUri"));
        Path resolved=Path.of(path).toAbsolutePath().normalize();
        if(!resolved.startsWith(registryRoot)||!Files.exists(resolved))throw new IllegalStateException("Model artifact is missing or outside registry root");
        PipelineModel pipeline=PipelineModel.load(resolved.toString());
        List<String> jsonRows=rows.stream().map(JSON::valueToTree).map(Object::toString).toList();
        Dataset<Row> input=spark.read().json(spark.createDataset(jsonRows,Encoders.STRING()));
        List<String> predictions=pipeline.transform(input).toJSON().collectAsList();
        return Map.of("modelId",modelId.toString(),"predictions",predictions,"rowCount",predictions.size());
    }

    private void insertExperiment(UUID id,String name,String task,String label,String features)throws SQLException{
        try(Connection c=connection();PreparedStatement p=c.prepareStatement("INSERT INTO automl_experiment(id,name,task_type,label_column,feature_columns,status) VALUES(?,?,?,?,?::jsonb,'QUEUED')")){
            p.setObject(1,id);p.setString(2,name);p.setString(3,task);p.setString(4,label);p.setString(5,features);p.executeUpdate();
        }
    }
    private void updateStatus(UUID id,String status,String algorithm,String metric,Double score,String uri)throws SQLException{
        try(Connection c=connection();PreparedStatement p=c.prepareStatement("UPDATE automl_experiment SET status=?,best_algorithm=?,best_metric=?,best_score=?,model_uri=?,finished_at=CASE WHEN ? IN ('SUCCEEDED','FAILED','CANCELLED') THEN now() ELSE finished_at END WHERE id=?")){
            p.setString(1,status);p.setString(2,algorithm);p.setString(3,metric);if(score==null)p.setNull(4,Types.DOUBLE);else p.setDouble(4,score);
            p.setString(5,uri);p.setString(6,status);p.setObject(7,id);p.executeUpdate();
        }
    }
    private void persistTrials(UUID id,List<Map<String,Object>> trials)throws SQLException{
        try(Connection c=connection();PreparedStatement p=c.prepareStatement("INSERT INTO automl_trial(experiment_id,trial_number,algorithm,parameters,metrics,status,duration_ms) VALUES(?,?,?,?::jsonb,?::jsonb,?,?)")){
            int index=0;for(Map<String,Object> t:trials){p.setObject(1,id);p.setInt(2,++index);p.setString(3,String.valueOf(t.get("algorithm")));
                p.setString(4,"{}");p.setString(5,JSONString(t));p.setString(6,String.valueOf(t.get("status")));
                p.setLong(7,((Number)t.getOrDefault("durationMs",0)).longValue());p.addBatch();}p.executeBatch();
        }
    }
    private void registerModel(UUID id,UUID experiment,String name,String algorithm,String task,String metric,double score,String uri,List<?> features)throws SQLException{
        try(Connection c=connection();PreparedStatement p=c.prepareStatement("INSERT INTO model_registry(id,experiment_id,name,algorithm,task_type,metric,best_score,model_uri,version,status,feature_columns) VALUES(?,?,?,?,?,?,?,?,1,'READY',?::jsonb)")){
            p.setObject(1,id);p.setObject(2,experiment);p.setString(3,name);p.setString(4,algorithm);p.setString(5,task);p.setString(6,metric);
            p.setDouble(7,score);p.setString(8,uri);p.setString(9,JSONString(features));p.executeUpdate();
        }
    }
    private Connection connection()throws SQLException{return DriverManager.getConnection(jdbcUrl,user,password);}
    private static String JSONString(Object value){try{return JSON.writeValueAsString(value);}catch(Exception e){throw new IllegalArgumentException(e);}}
    private static String env(String key,String fallback){return Optional.ofNullable(System.getenv(key)).filter(s->!s.isBlank()).orElse(fallback);}
}
