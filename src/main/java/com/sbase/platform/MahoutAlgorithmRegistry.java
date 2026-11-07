package com.sbase.platform;
import org.apache.mahout.math.DenseVector;
import org.springframework.stereotype.Component;
import java.util.*;
@Component public class MahoutAlgorithmRegistry {
 private final Map<String,MahoutAlgorithmAdapter> adapters;
 public MahoutAlgorithmRegistry(){Map<String,MahoutAlgorithmAdapter> m=new LinkedHashMap<>();
 m.put("cosine",new MahoutAlgorithmAdapter(){public String name(){return "cosine";}public double score(double[] a,double[] b){check(a,b);var x=new DenseVector(a);var y=new DenseVector(b);double n=Math.sqrt(x.dot(x)*y.dot(y));return n==0?0:x.dot(y)/n;}});
 m.put("euclidean",new MahoutAlgorithmAdapter(){public String name(){return "euclidean";}public double score(double[] a,double[] b){check(a,b);double s=0;for(int i=0;i<a.length;i++){double d=a[i]-b[i];s+=d*d;}return Math.sqrt(s);}});
 adapters=Collections.unmodifiableMap(m);}
 private static void check(double[] a,double[] b){if(a==null||b==null||a.length==0||a.length!=b.length)throw new IllegalArgumentException("Vectors must be non-empty and have equal dimensions");for(double v:a)if(!Double.isFinite(v))throw new IllegalArgumentException("Values must be finite");for(double v:b)if(!Double.isFinite(v))throw new IllegalArgumentException("Values must be finite");}
 public Set<String> algorithms(){return adapters.keySet();}
 public double score(String name,double[] a,double[] b){var adapter=adapters.get(name.toLowerCase(Locale.ROOT));if(adapter==null)throw new IllegalArgumentException("Unknown algorithm: "+name);return adapter.score(a,b);}
}