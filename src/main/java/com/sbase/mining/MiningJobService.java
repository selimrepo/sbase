package com.sbase.mining;

import com.sbase.doris.DorisDatasetWriter;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.Executor;

import static org.apache.spark.sql.functions.col;

@Service
public class MiningJobService {
    private final MiningJobRepository repository;
    private final SparkSession spark;
    private final DorisDatasetWriter dorisWriter;
    private final Executor executor;

    public MiningJobService(MiningJobRepository repository, SparkSession spark,
                            DorisDatasetWriter dorisWriter,
                            @Qualifier("miningExecutor") Executor executor) {
        this.repository = repository;
        this.spark = spark;
        this.dorisWriter = dorisWriter;
        this.executor = executor;
    }

    @Transactional
    public MiningJob submit(MiningJobRequest request) {
        MiningJobEntity entity = repository.save(new MiningJobEntity(
                UUID.randomUUID(), request.operation(), request.sourceTable(), request.outputTable()));
        executor.execute(() -> execute(entity.getId(), request));
        return MiningJob.from(entity);
    }

    @Transactional(readOnly = true)
    public MiningJob find(UUID id) {
        return repository.findById(id).map(MiningJob::from).orElse(null);
    }

    private void execute(UUID id, MiningJobRequest request) {
        try {
            markRunning(id);
            Dataset<Row> source = spark.read()
                    .format("jdbc")
                    .option("url", System.getenv().getOrDefault("DORIS_JDBC_URL",
                            "jdbc:mysql://doris-fe:9030/analytics?useSSL=false&allowPublicKeyRetrieval=true"))
                    .option("dbtable", request.sourceTable())
                    .option("user", System.getenv().getOrDefault("DORIS_USERNAME", "root"))
                    .option("password", System.getenv().getOrDefault("DORIS_PASSWORD", ""))
                    .option("driver", "com.mysql.cj.jdbc.Driver")
                    .load();

            String summary = switch (request.operation()) {
                case "describe" -> source.describe().toJSON().collectAsList().toString();
                case "profile" -> source.summary().toJSON().collectAsList().toString();
                case "kmeans" -> runKMeans(source, request);
                default -> throw new IllegalArgumentException("Unsupported operation: " + request.operation());
            };
            markSucceeded(id, summary);
        } catch (Exception ex) {
            markFailed(id, ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage());
        }
    }

    private String runKMeans(Dataset<Row> source, MiningJobRequest request) {
        String[] features = Arrays.stream(source.schema().fieldNames())
                .filter(name -> {
                    String type = source.schema().apply(name).dataType().typeName();
                    return type.equals("double") || type.equals("float")
                            || type.equals("integer") || type.equals("long");
                }).toArray(String[]::new);
        if (features.length == 0) throw new IllegalArgumentException("K-means requires numeric columns");

        Dataset<Row> vectors = new VectorAssembler().setInputCols(features)
                .setOutputCol("features").setHandleInvalid("skip").transform(source);
        KMeansModel model = new KMeans().setK(request.k() == null ? 3 : request.k())
                .setMaxIter(request.maxIterations() == null ? 20 : request.maxIterations())
                .setSeed(42L).fit(vectors);
        Dataset<Row> predictions = model.transform(vectors).drop("features");
        dorisWriter.append(predictions, request.outputTable());
        return "rows=" + predictions.count() + ", features=" + Arrays.toString(features)
                + ", trainingCost=" + model.summary().trainingCost()
                + ", clusterCenters=" + Arrays.toString(model.clusterCenters());
    }

    private void markRunning(UUID id) {
        repository.findById(id).ifPresent(job -> { job.start(); repository.save(job); });
    }
    private void markSucceeded(UUID id, String summary) {
        repository.findById(id).ifPresent(job -> { job.succeed(summary); repository.save(job); });
    }
    private void markFailed(UUID id, String error) {
        repository.findById(id).ifPresent(job -> { job.fail(error); repository.save(job); });
    }
}
