package com.sbase.mining;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sbase.doris.DorisDatasetWriter;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.springframework.stereotype.Service;
import java.util.*;
import static org.apache.spark.sql.functions.*;

@Service
public class AutomaticMiningService {
 private final ObjectMapper json; private final DorisDatasetWriter writer;
 public AutomaticMiningService(ObjectMapper json,DorisDatasetWriter writer){this.json=json;this.writer=writer;}

 public String analyze(Dataset<Row> input,String outputTable){
  long rows=input.count();
  if(rows==0)throw new IllegalArgumentException("Automatic mining cannot analyze an empty dataset");
  StructField[] fields=input.schema().fields();
  List<String> numeric=new ArrayList<>(),categorical=new ArrayList<>(),temporal=new ArrayList<>();
  Map<String,Object> columns=new LinkedHashMap<>();
  for(StructField f:fields){
   String t=f.dataType().typeName();
   if(Set.of("byte","short","integer","long","float","double","decimal").contains(t))numeric.add(f.name());
   else if(t.equals("date")||t.equals("timestamp"))temporal.add(f.name());
   else if(t.equals("string")||t.equals("boolean"))categorical.add(f.name());
  }
  List<Column> missingExpr=new ArrayList<>();
  for(StructField f:fields)missingExpr.add(sum(when(col(f.name()).isNull(),1).otherwise(0)).alias(f.name()));
  Row missing=input.agg(missingExpr.get(0),missingExpr.subList(1,missingExpr.size()).toArray(Column[]::new)).first();
  for(int i=0;i<fields.length;i++){
   StructField f=fields[i];Map<String,Object> m=new LinkedHashMap<>();
   m.put("type",f.dataType().simpleString());m.put("nullCount",missing.getLong(i));m.put("nullRate",missing.getLong(i)/(double)rows);
   m.put("distinctApprox",input.select(col(f.name())).agg(approx_count_distinct(col(f.name())).alias("n")).first().getLong(0));
   if(numeric.contains(f.name())){
    Row s=input.select(avg(col(f.name()).cast("double")),stddev_pop(col(f.name()).cast("double")),min(col(f.name())),max(col(f.name()))).first();
    m.put("mean",s.isNullAt(0)?null:s.getDouble(0));m.put("stddev",s.isNullAt(1)?null:s.getDouble(1));
    m.put("min",s.isNullAt(2)?null:String.valueOf(s.get(2)));m.put("max",s.isNullAt(3)?null:String.valueOf(s.get(3)));
    double[] q=input.stat().approxQuantile(f.name(),new double[]{0.25,0.5,0.75},0.02);
    if(q.length==3){double iqr=q[2]-q[0];m.put("median",q[1]);m.put("outlierLowerFence",q[0]-1.5*iqr);m.put("outlierUpperFence",q[2]+1.5*iqr);}
   } else if(categorical.contains(f.name())){
    List<Row> top=input.groupBy(col(f.name())).count().orderBy(desc("count")).limit(10).collectAsList();
    m.put("topValues",top.stream().map(x->Map.of("value",String.valueOf(x.get(0)),"count",x.getLong(1))).toList());
   }
   columns.put(f.name(),m);
  }
  Map<String,Object> correlations=new LinkedHashMap<>();
  List<String> selected=numeric.stream().limit(20).toList();
  for(int i=0;i<selected.size();i++)for(int j=i+1;j<selected.size();j++){
   String a=selected.get(i),b=selected.get(j);
   double corr=input.stat().corr(a,b);
   if(Double.isFinite(corr)&&Math.abs(corr)>=0.5)correlations.put(a+"__"+b,corr);
  }
  Map<String,Object> report=new LinkedHashMap<>();
  report.put("rows",rows);report.put("columnCount",fields.length);report.put("numericColumns",numeric);
  report.put("categoricalColumns",categorical);report.put("temporalColumns",temporal);
  report.put("columns",columns);report.put("strongCorrelations",correlations);
  report.put("recommendations",recommend(rows,numeric,categorical,columns,correlations));
  if(numeric.size()>=2&&rows>=10&&!outputTable.isBlank()){
   String[] features=numeric.stream().limit(50).toArray(String[]::new);
   Dataset<Row> vectors=new VectorAssembler().setInputCols(features).setOutputCol("features").setHandleInvalid("skip").transform(input);
   int k=Math.max(2,Math.min(5,(int)Math.sqrt(rows/2.0)));
   KMeansModel model=new KMeans().setK(k).setMaxIter(30).setSeed(42L).fit(vectors);
   Dataset<Row> predictions=model.transform(vectors).drop("features");
   writer.append(predictions,outputTable);
   report.put("clustering",Map.of("algorithm","kmeans","k",k,"trainingCost",model.summary().trainingCost(),"features",features,"outputTable",outputTable));
  } else report.put("clustering",Map.of("skipped",true,"reason","At least two numeric columns and ten rows are required"));
  try{return json.writeValueAsString(report);}catch(JsonProcessingException e){throw new IllegalStateException("Could not serialize mining report",e);}
 }
 private List<String> recommend(long rows,List<String> numeric,List<String> categorical,Map<String,Object> columns,Map<String,Object> correlations){
  List<String> r=new ArrayList<>();
  if(numeric.size()>=2)r.add("Numeric feature set detected: clustering and correlation analysis are applicable");
  if(!categorical.isEmpty())r.add("Categorical features detected: encode or index them before supervised learning");
  if(!correlations.isEmpty())r.add("Strong feature correlations detected: inspect multicollinearity before regression");
  boolean missing=columns.values().stream().map(x->(Map<?,?>)x).anyMatch(x->((Number)x.get("nullRate")).doubleValue()>0.05);
  if(missing)r.add("Columns with more than 5% missing values need an explicit imputation strategy");
  if(rows<100)r.add("Small dataset: validate results with cross-validation and avoid overfitting");
  if(r.isEmpty())r.add("No immediate data quality warning detected; consider task-specific supervised modeling");
  return r;
 }
}
