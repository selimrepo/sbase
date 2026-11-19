package io.sbase.server.automl;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Bounded, deterministic statistical mining primitives for tabular datasets.
 * The service deliberately returns method names, sample sizes and caveats with every result.
 */
public final class ScientificMiningService {
    private static final int MAX_ROWS = 100_000;
    private static final int MAX_FEATURES = 50;

    public Map<String, Object> associations(Map<String, Object> request) {
        List<Map<String, Object>> rows = rows(request.get("data"));
        List<String> features = features(request.get("features"), rows);
        if (features.size() < 2) throw new IllegalArgumentException("At least two features are required");
        if (features.size() > MAX_FEATURES) throw new IllegalArgumentException("At most 50 features can be analyzed at once");

        List<Map<String, Object>> results = new ArrayList<>();
        for (int i = 0; i < features.size(); i++) {
            for (int j = i + 1; j < features.size(); j++) {
                String left = features.get(i), right = features.get(j);
                List<Object> x = new ArrayList<>(), y = new ArrayList<>();
                for (Map<String, Object> row : rows) {
                    Object a = row.get(left), b = row.get(right);
                    if (!isBlank(a) && !isBlank(b)) {
                        x.add(a); y.add(b);
                    }
                }
                if (x.size() < 3) continue;
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("featureA", left); item.put("featureB", right); item.put("sampleSize", x.size());
                if (allNumeric(x) && allNumeric(y)) {
                    List<Double> a = numbers(x), b = numbers(y);
                    double pearson = pearson(a, b), spearman = pearson(ranks(a), ranks(b));
                    double p = correlationPValue(spearman, a.size());
                    item.put("method", "PEARSON_AND_SPEARMAN");
                    item.put("pearsonR", pearson); item.put("spearmanRho", spearman);
                    item.put("pValue", p);
                } else if (!allNumeric(x) && !allNumeric(y)) {
                    Association association = categoricalAssociation(x, y);
                    item.put("method", "CRAMERS_V_CHI_SQUARE");
                    item.put("cramersV", association.score); item.put("pValue", association.pValue);
                    item.put("degreesOfFreedom", association.degreesOfFreedom);
                } else {
                    List<Double> numeric = numbers(allNumeric(x) ? x : y);
                    List<Object> categorical = allNumeric(x) ? y : x;
                    Association association = correlationRatio(categorical, numeric);
                    item.put("method", "CORRELATION_RATIO_ETA_SQUARED");
                    item.put("etaSquared", association.score); item.put("pValue", association.pValue);
                    item.put("degreesOfFreedom", association.degreesOfFreedom);
                }
                results.add(item);
            }
        }
        adjustBenjaminiHochberg(results);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("method", "PAIRWISE_ASSOCIATION_WITH_BENJAMINI_HOCHBERG_FDR");
        response.put("rows", rows.size()); response.put("testedPairs", results.size());
        response.put("associations", results);
        response.put("notes", List.of(
                "Pearson and Spearman are association measures, not causal effects.",
                "Categorical associations use Pearson chi-square and Cramer's V.",
                "Mixed numeric/categorical associations use correlation ratio eta-squared and an approximate F test.",
                "P-values are adjusted across tested pairs with Benjamini-Hochberg false-discovery-rate control."
        ));
        return response;
    }

    public Map<String, Object> selectFeatures(Map<String, Object> request) {
        List<Map<String, Object>> rows = rows(request.get("data"));
        String target = required(request.get("target"), "target");
        if (rows.stream().noneMatch(r -> r.containsKey(target))) throw new IllegalArgumentException("Unknown target column: " + target);
        List<Double> targetValues = new ArrayList<>();
        List<Map<String, Object>> output = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Object value = row.get(target);
            if (value instanceof Number n && Double.isFinite(n.doubleValue())) targetValues.add(n.doubleValue());
        }
        if (targetValues.size() < 3) throw new IllegalArgumentException("Numeric target must have at least three non-null observations");
        List<String> features = features(request.get("features"), rows).stream().filter(f -> !f.equals(target)).toList();
        if (features.isEmpty()) throw new IllegalArgumentException("No candidate features remain after excluding target");
        if (features.size() > MAX_FEATURES) throw new IllegalArgumentException("At most 50 features can be selected at once");

        for (String feature : features) {
            List<Double> x = new ArrayList<>(), y = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                Object a = row.get(feature), b = row.get(target);
                if (a instanceof Number na && b instanceof Number nb
                        && Double.isFinite(na.doubleValue()) && Double.isFinite(nb.doubleValue())) {
                    x.add(na.doubleValue()); y.add(nb.doubleValue());
                }
            }
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("feature", feature); item.put("sampleSize", x.size());
            if (x.size() < 3) {
                item.put("status", "INSUFFICIENT_NUMERIC_PAIRS"); item.put("score", 0.0); item.put("pValue", 1.0);
            } else {
                double variance = variance(x);
                double rho = pearson(ranks(x), ranks(y));
                item.put("status", variance <= 1e-15 ? "CONSTANT_FEATURE" : "EVALUATED");
                item.put("method", "SPEARMAN_RANK_CORRELATION");
                item.put("score", Math.abs(rho)); item.put("signedSpearmanRho", rho);
                item.put("pValue", variance <= 1e-15 ? 1.0 : correlationPValue(rho, x.size()));
            }
            output.add(item);
        }
        adjustBenjaminiHochberg(output);
        double alpha = number(request.get("fdrAlpha"), 0.05);
        double minimumScore = number(request.get("minimumAbsoluteSpearman"), 0.05);
        if (!(alpha > 0 && alpha < 1) || minimumScore < 0 || minimumScore > 1)
            throw new IllegalArgumentException("fdrAlpha must be in (0,1) and minimumAbsoluteSpearman in [0,1]");
        output.sort((a, b) -> Double.compare(((Number)b.get("score")).doubleValue(), ((Number)a.get("score")).doubleValue()));
        List<String> selected = output.stream().filter(v -> "EVALUATED".equals(v.get("status"))
                && ((Number)v.get("qValue")).doubleValue() <= alpha
                && ((Number)v.get("score")).doubleValue() >= minimumScore)
                .map(v -> String.valueOf(v.get("feature"))).toList();
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("target", target); response.put("method", "UNIVARIATE_SPEARMAN_FILTER_WITH_BH_FDR");
        response.put("fdrAlpha", alpha); response.put("minimumAbsoluteSpearman", minimumScore);
        response.put("selectedFeatures", selected); response.put("rankedFeatures", output);
        response.put("notes", List.of(
                "This is a univariate filter, not a substitute for nested cross-validation or domain review.",
                "Only numeric candidate features and a numeric target are evaluated in this version.",
                "Correlated features can be redundant; evaluate selected sets inside the training folds to avoid selection leakage."
        ));
        return response;
    }

    public Map<String, Object> compareGroups(Map<String, Object> request) {
        List<Map<String, Object>> rows = rows(request.get("data"));
        String groupColumn = required(request.get("groupColumn"), "groupColumn");
        String valueColumn = required(request.get("valueColumn"), "valueColumn");
        String groupA = required(request.get("groupA"), "groupA");
        String groupB = required(request.get("groupB"), "groupB");
        if (groupA.equals(groupB)) throw new IllegalArgumentException("groupA and groupB must be different");
        List<Double> a = new ArrayList<>(), b = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            Object group = row.get(groupColumn), value = row.get(valueColumn);
            if (!(value instanceof Number n) || !Double.isFinite(n.doubleValue())) continue;
            if (Objects.equals(String.valueOf(group), groupA)) a.add(n.doubleValue());
            if (Objects.equals(String.valueOf(group), groupB)) b.add(n.doubleValue());
        }
        if (a.size() < 2 || b.size() < 2) throw new IllegalArgumentException("Each group must contain at least two numeric observations");
        double meanA = mean(a), meanB = mean(b), varA = variance(a), varB = variance(b);
        double se2 = varA / a.size() + varB / b.size();
        if (se2 <= 0) throw new IllegalArgumentException("Both groups have zero pooled sampling variance");
        double t = (meanA - meanB) / Math.sqrt(se2);
        double df = se2 * se2 / (Math.pow(varA / a.size(), 2) / (a.size() - 1)
                + Math.pow(varB / b.size(), 2) / (b.size() - 1));
        double p = studentTwoSidedP(t, df);
        double critical = studentCritical95(df);
        double margin = critical * Math.sqrt(se2);
        double pooledSd = Math.sqrt(((a.size()-1)*varA + (b.size()-1)*varB) / (a.size()+b.size()-2));
        double correction = 1.0 - 3.0 / (4.0*(a.size()+b.size()) - 9.0);
        double hedgesG = pooledSd == 0 ? 0 : ((meanA - meanB) / pooledSd) * correction;
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("method", "WELCH_TWO_SAMPLE_T_TEST");
        response.put("groupA", Map.of("name", groupA, "n", a.size(), "mean", meanA, "standardDeviation", Math.sqrt(varA)));
        response.put("groupB", Map.of("name", groupB, "n", b.size(), "mean", meanB, "standardDeviation", Math.sqrt(varB)));
        response.put("meanDifferenceAminusB", meanA - meanB);
        response.put("tStatistic", t); response.put("degreesOfFreedom", df); response.put("pValueTwoSided", p);
        response.put("confidenceLevel", 0.95);
        response.put("confidenceInterval", Map.of("lower", meanA-meanB-margin, "upper", meanA-meanB+margin));
        response.put("hedgesG", hedgesG);
        response.put("interpretation", p < 0.05 ? "STATISTICALLY_SIGNIFICANT_AT_0_05" : "NOT_SIGNIFICANT_AT_0_05");
        response.put("notes", List.of("Welch's test does not assume equal group variances.",
                "The confidence interval uses a Student-t critical value and is an interval for the mean difference.",
                "Statistical significance does not establish practical importance or causality."));
        return response;
    }

    private static List<Map<String, Object>> rows(Object raw) {
        if (!(raw instanceof List<?> list) || list.isEmpty() || list.size() > MAX_ROWS)
            throw new IllegalArgumentException("data must contain between 1 and " + MAX_ROWS + " rows");
        List<Map<String, Object>> result = new ArrayList<>(list.size());
        for (Object item : list) {
            if (!(item instanceof Map<?, ?> map)) throw new IllegalArgumentException("Every data row must be an object");
            Map<String, Object> row = new LinkedHashMap<>();
            map.forEach((k, v) -> row.put(String.valueOf(k), v));
            result.add(row);
        }
        return result;
    }

    private static List<String> features(Object raw, List<Map<String, Object>> rows) {
        List<String> result = raw instanceof List<?> list ? list.stream().map(String::valueOf).distinct().toList()
                : rows.get(0).keySet().stream().toList();
        if (result.isEmpty()) throw new IllegalArgumentException("features must not be empty");
        Set<String> available = rows.stream().flatMap(r -> r.keySet().stream()).collect(Collectors.toSet());
        if (!available.containsAll(result)) throw new IllegalArgumentException("Unknown feature column");
        return result;
    }

    private static boolean isBlank(Object value) { return value == null || (value instanceof String text && text.isBlank()); }

    private static String required(Object value, String name) {
        if (value == null || String.valueOf(value).isBlank()) throw new IllegalArgumentException(name + " is required");
        return String.valueOf(value);
    }
    private static double number(Object value, double fallback) { return value instanceof Number n ? n.doubleValue() : fallback; }
    private static boolean allNumeric(List<?> values) {
        return values.stream().allMatch(v -> v instanceof Number n && Double.isFinite(n.doubleValue()));
    }
    private static List<Double> numbers(List<?> values) { return values.stream().map(v -> ((Number)v).doubleValue()).toList(); }
    private static double mean(List<Double> v) { return v.stream().mapToDouble(Double::doubleValue).average().orElse(0); }
    private static double variance(List<Double> v) {
        if (v.size() < 2) return 0;
        double m = mean(v); return v.stream().mapToDouble(x -> (x-m)*(x-m)).sum()/(v.size()-1);
    }
    private static double pearson(List<Double> x, List<Double> y) {
        double mx=mean(x), my=mean(y), cov=0, sx=0, sy=0;
        for(int i=0;i<x.size();i++){double dx=x.get(i)-mx,dy=y.get(i)-my;cov+=dx*dy;sx+=dx*dx;sy+=dy*dy;}
        return sx<=0 || sy<=0 ? 0 : Math.max(-1,Math.min(1,cov/Math.sqrt(sx*sy)));
    }
    private static List<Double> ranks(List<Double> values) {
        List<Integer> order = new ArrayList<>(); for(int i=0;i<values.size();i++) order.add(i);
        order.sort(Comparator.comparingDouble(values::get));
        double[] ranks = new double[values.size()];
        for(int i=0;i<order.size();){
            int j=i+1; while(j<order.size() && Double.compare(values.get(order.get(i)),values.get(order.get(j)))==0) j++;
            double rank=(i+1+j)/2.0; for(int k=i;k<j;k++) ranks[order.get(k)]=rank; i=j;
        }
        List<Double> result=new ArrayList<>(); for(double rank:ranks) result.add(rank); return result;
    }
    private record Association(double score, double pValue, int degreesOfFreedom) {}
    private static Association categoricalAssociation(List<Object> x, List<Object> y) {
        Map<String,Integer> xi=new LinkedHashMap<>(), yi=new LinkedHashMap<>();
        for(Object v:x) xi.computeIfAbsent(String.valueOf(v),k->xi.size());
        for(Object v:y) yi.computeIfAbsent(String.valueOf(v),k->yi.size());
        int[][] table=new int[xi.size()][yi.size()]; int[] row=new int[xi.size()], col=new int[yi.size()];
        for(int i=0;i<x.size();i++){int a=xi.get(String.valueOf(x.get(i))),b=yi.get(String.valueOf(y.get(i)));table[a][b]++;row[a]++;col[b]++;}
        double chi=0; for(int i=0;i<row.length;i++)for(int j=0;j<col.length;j++){double e=(double)row[i]*col[j]/x.size();if(e>0)chi+=Math.pow(table[i][j]-e,2)/e;}
        int df=(row.length-1)*(col.length-1); double v=Math.sqrt(chi/(x.size()*Math.max(1,Math.min(row.length-1,col.length-1))));
        return new Association(v,df==0?1:gammaQ(df/2.0,chi/2.0),df);
    }
    private static Association correlationRatio(List<Object> categories, List<Double> values) {
        Map<String,List<Double>> groups=new LinkedHashMap<>();
        for(int i=0;i<categories.size();i++)groups.computeIfAbsent(String.valueOf(categories.get(i)),k->new ArrayList<>()).add(values.get(i));
        double overall=mean(values), total=0, between=0;
        for(double v:values)total+=(v-overall)*(v-overall);
        for(List<Double> group:groups.values()){double delta=mean(group)-overall;between+=group.size()*delta*delta;}
        double eta=total<=0?0:Math.sqrt(Math.max(0,Math.min(1,between/total)));
        int df1=groups.size()-1,df2=values.size()-groups.size();
        double f=df1<=0||df2<=0?0:(total<=between?Double.POSITIVE_INFINITY:(between/df1)/((total-between)/df2));
        double p=df1<=0||df2<=0?1:regularizedBeta(df2/(df2+df1*f),df2/2.0,df1/2.0);
        return new Association(eta,p,df1);
    }
    private static double correlationPValue(double r,int n) {
        if(n<=2)return 1; double bounded=Math.min(0.999999999,Math.abs(r));
        double t=bounded*Math.sqrt((n-2)/(1-bounded*bounded)); return studentTwoSidedP(t,n-2);
    }
    private static void adjustBenjaminiHochberg(List<Map<String,Object>> items) {
        List<Map<String,Object>> sorted=new ArrayList<>(items);
        sorted.sort(Comparator.comparingDouble(m->((Number)m.get("pValue")).doubleValue()));
        double next=1; int m=sorted.size();
        for(int i=m-1;i>=0;i--){double p=((Number)sorted.get(i).get("pValue")).doubleValue();next=Math.min(next,p*m/(i+1.0));sorted.get(i).put("qValue",Math.min(1,next));}
    }
    private static double studentTwoSidedP(double t,double df) {
        if(!Double.isFinite(t)||df<=0)return 1; return regularizedBeta(df/(df+t*t),df/2.0,0.5);
    }
    private static double studentCritical95(double df) {
        double low=0,high=20; for(int i=0;i<80;i++){double mid=(low+high)/2;if(studentTwoSidedP(mid,df)>0.05)low=mid;else high=mid;}return (low+high)/2;
    }
    private static double regularizedBeta(double x,double a,double b) {
        if(x<=0)return 0;if(x>=1)return 1;
        double bt=Math.exp(logGamma(a+b)-logGamma(a)-logGamma(b)+a*Math.log(x)+b*Math.log1p(-x));
        if(x<(a+1)/(a+b+2))return bt*betaFraction(x,a,b)/a;
        return 1-bt*betaFraction(1-x,b,a)/b;
    }
    private static double betaFraction(double x,double a,double b) {
        final double tiny=1e-300, eps=3e-14; double qab=a+b,qap=a+1,qam=a-1,c=1,d=1-qab*x/qap;
        if(Math.abs(d)<tiny)d=tiny;d=1/d;double h=d;
        for(int m=1;m<=300;m++){int m2=2*m;double aa=m*(b-m)*x/((qam+m2)*(a+m2));d=1+aa*d;if(Math.abs(d)<tiny)d=tiny;c=1+aa/c;if(Math.abs(c)<tiny)c=tiny;d=1/d;h*=d*c;
            aa=-(a+m)*(qab+m)*x/((a+m2)*(qap+m2));d=1+aa*d;if(Math.abs(d)<tiny)d=tiny;c=1+aa/c;if(Math.abs(c)<tiny)c=tiny;d=1/d;double delta=d*c;h*=delta;if(Math.abs(delta-1)<eps)break;}
        return h;
    }
    private static double gammaQ(double a,double x) {
        if(x<=0)return 1;if(x<a+1){double sum=1/a,term=sum,ap=a;for(int n=1;n<=500;n++){term*=x/++ap;sum+=term;if(Math.abs(term)<Math.abs(sum)*1e-14)break;}return 1-sum*Math.exp(-x+a*Math.log(x)-logGamma(a));}
        double b=x+1-a,c=1e300,d=1/b,h=d;for(int i=1;i<=500;i++){double an=-i*(i-a);b+=2;d=an*d+b;if(Math.abs(d)<1e-300)d=1e-300;c=b+an/c;if(Math.abs(c)<1e-300)c=1e-300;d=1/d;double delta=d*c;h*=delta;if(Math.abs(delta-1)<1e-14)break;}return Math.exp(-x+a*Math.log(x)-logGamma(a))*h;
    }
    private static double logGamma(double z) {
        double[] p={676.5203681218851,-1259.1392167224028,771.3234287776531,-176.6150291621406,12.507343278686905,-0.13857109526572012,9.984369578019572e-6,1.5056327351493116e-7};
        if(z<0.5)return Math.log(Math.PI)-Math.log(Math.sin(Math.PI*z))-logGamma(1-z);
        z-=1;double x=0.99999999999980993;for(int i=0;i<p.length;i++)x+=p[i]/(z+i+1);double t=z+p.length-0.5;return 0.5*Math.log(2*Math.PI)+(z+0.5)*Math.log(t)-t+Math.log(x);
    }
}
