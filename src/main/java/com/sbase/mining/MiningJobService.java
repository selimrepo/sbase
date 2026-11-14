package com.sbase.mining;

import com.sbase.postgres.PostgresClient;
import com.sbase.postgres.PostgresDatasetWriter;
import com.sbase.platform.bi.PostgresTenantTablePolicy;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Executor;

@Service
public class MiningJobService {
    private final MiningJobRepository repository;
    private final SparkSession spark;
    private final PostgresClient postgres;
    private final PostgresDatasetWriter postgresWriter;
    private final PostgresTenantTablePolicy tablePolicy;
    private final Executor executor;
    private final AutomaticMiningService automaticMining;
    public MiningJobService(MiningJobRepository repository,@Lazy SparkSession spark,PostgresClient postgres,
            PostgresDatasetWriter postgresWriter,PostgresTenantTablePolicy tablePolicy,AutomaticMiningService automaticMining,@Qualifier("miningExecutor") Executor executor) {
        this.repository=repository; this.spark=spark; this.postgres=postgres; this.postgresWriter=postgresWriter; this.tablePolicy=tablePolicy;
        this.automaticMining=automaticMining; this.executor=executor;
    }
    @Transactional public MiningJob submit(MiningJobRequest request,UUID tenantId,String subject) {
        MiningJobEntity entity=repository.saveAndFlush(new MiningJobEntity(UUID.randomUUID(),tenantId,subject,
            request.operation(),request.sourceTable(),request.outputTable()));
        UUID id=entity.getId();
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() {
                try { executor.execute(() -> execute(id,request)); }
                catch(RuntimeException ex) { markFailed(id,"Mining queue rejected the job: "+ex.getMessage()); }
            }
        });
        return MiningJob.from(entity);
    }
    @Transactional(readOnly=true) public MiningJob find(UUID id,UUID tenantId) {
        return repository.findByIdAndTenantId(id,tenantId).map(MiningJob::from).orElse(null);
    }
    private void execute(UUID id,MiningJobRequest request) {
        try {
            markRunning(id);
            UUID tenantId=repository.findById(id).map(MiningJobEntity::getTenantId)
                .orElseThrow(() -> new IllegalStateException("Mining job disappeared before execution"));
            String sourceTable=tablePolicy.requireTenantScoped(request.sourceTable());
            String scopedSource="(SELECT * FROM "+sourceTable+" WHERE tenant_id = '"+tenantId+"') AS sbase_tenant_source";
            Dataset<Row> source=spark.read().format("jdbc").option("url",postgres.jdbcUrl()).option("dbtable",scopedSource)
                .option("user",postgres.username()).option("password",postgres.password()).option("driver","org.postgresql.Driver").load();
            String summary=switch(request.operation()) {
                case "describe" -> source.describe().toJSON().collectAsList().toString();
                case "profile" -> source.summary().toJSON().collectAsList().toString();
                case "auto" -> automaticMining.analyze(source,request.outputTable());
                case "kmeans" -> runKMeans(source,request,tenantId);
                default -> throw new IllegalArgumentException("Unsupported operation: "+request.operation());
            };
            markSucceeded(id,summary);
        } catch(Exception ex) { markFailed(id,ex.getMessage()==null?ex.getClass().getSimpleName():ex.getMessage()); }
    }
    private String runKMeans(Dataset<Row> source,MiningJobRequest request,UUID tenantId) {
        String[] features=Arrays.stream(source.schema().fieldNames()).filter(name -> {
            String type=source.schema().apply(name).dataType().typeName();
            return type.equals("double")||type.equals("float")||type.equals("integer")||type.equals("long");
        }).toArray(String[]::new);
        if(features.length==0) throw new IllegalArgumentException("K-means requires numeric columns");
        Dataset<Row> vectors=new VectorAssembler().setInputCols(features).setOutputCol("features").setHandleInvalid("skip").transform(source);
        KMeansModel model=new KMeans().setK(request.k()==null?3:request.k()).setMaxIter(request.maxIterations()==null?20:request.maxIterations()).setSeed(42L).fit(vectors);
        Dataset<Row> predictions=model.transform(vectors).drop("features");
        if (!Arrays.asList(predictions.columns()).contains("tenant_id"))
            throw new org.springframework.security.access.AccessDeniedException("Mining source must retain tenant_id through model output");
        postgresWriter.append(predictions,request.outputTable(),tenantId);
        return "rows="+predictions.count()+", features="+Arrays.toString(features)+", trainingCost="+model.summary().trainingCost()
            +", clusterCenters="+Arrays.toString(model.clusterCenters());
    }
    private void markRunning(UUID id){repository.findById(id).ifPresent(job->{job.start();repository.save(job);});}
    private void markSucceeded(UUID id,String summary){repository.findById(id).ifPresent(job->{job.succeed(summary);repository.save(job);});}
    private void markFailed(UUID id,String error){repository.findById(id).ifPresent(job->{job.fail(error);repository.save(job);});}
}
