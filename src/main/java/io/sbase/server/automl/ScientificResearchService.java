package io.sbase.server.automl;

import java.util.*;

/**
 * Bounded statistical research methods. Every method reports its estimator,
 * sample sizes and material assumptions; these are analysis aids, not causal proof.
 */
public final class ScientificResearchService {
    private static final int MAX_ROWS = 100_000;

    public Map<String,Object> mannWhitneyU(Map<String,Object> request) {
        List<Map<String,Object>> rows=rows(request.get("data"));
        String groupColumn=required(request,"groupColumn"), valueColumn=required(request,"valueColumn");
        String groupA=required(request,"groupA"), groupB=required(request,"groupB");
        if(groupA.equals(groupB)) throw new IllegalArgumentException("groupA and groupB must differ");
        List<Double> a=new ArrayList<>(), b=new ArrayList<>();
        for(Map<String,Object> row:rows) {
            Object raw=row.get(valueColumn), group=row.get(groupColumn);
            if(!(raw instanceof Number n) || !Double.isFinite(n.doubleValue())) continue;
            if(Objects.equals(String.valueOf(group),groupA)) a.add(n.doubleValue());
            else if(Objects.equals(String.valueOf(group),groupB)) b.add(n.doubleValue());
        }
        if(a.size()<2 || b.size()<2) throw new IllegalArgumentException("Each group needs at least two numeric observations");
        List<Double> combined=new ArrayList<>(a); combined.addAll(b);
        List<Double> ranks=ranks(combined);
        double rankA=0; for(int i=0;i<a.size();i++) rankA+=ranks.get(i);
        double uA=rankA-a.size()*(a.size()+1.0)/2.0, uB=(double)a.size()*b.size()-uA;
        Map<Double,Integer> ties=new HashMap<>(); for(double v:combined) ties.merge(v,1,Integer::sum);
        double tieSum=ties.values().stream().filter(t->t>1).mapToDouble(t->(double)t*t*t-t).sum();
        double n=combined.size(), variance=(double)a.size()*b.size()/12.0*
                (n+1.0-tieSum/(n*(n-1.0)));
        if(variance<=0) throw new IllegalArgumentException("All observations are tied; Mann-Whitney variance is zero");
        double delta=Math.abs(uA-(double)a.size()*b.size()/2.0);
        double z=Math.copySign(Math.max(0,delta-0.5)/Math.sqrt(variance),uA-(double)a.size()*b.size()/2.0);
        double p=Math.min(1,2*(1-normalCdf(Math.abs(z))));
        Map<String,Object> out=new LinkedHashMap<>();
        out.put("method","MANN_WHITNEY_U_ASYMPTOTIC_TWO_SIDED");
        out.put("groupA",Map.of("name",groupA,"n",a.size(),"median",median(a)));
        out.put("groupB",Map.of("name",groupB,"n",b.size(),"median",median(b)));
        out.put("uStatistic",uA); out.put("uStatisticOtherGroup",uB);
        out.put("zStatistic",z); out.put("pValueTwoSided",p);
        out.put("rankBiserialCorrelation",2*uA/((double)a.size()*b.size())-1);
        out.put("notes",List.of("Uses a tie-corrected normal approximation with continuity correction; exact small-sample inference is not computed.",
                "The test compares rank distributions; it is a test of medians only under additional equal-shape distribution assumptions.",
                "Independent observations and an appropriate sampling design are required."));
        return out;
    }

    public Map<String,Object> kruskalWallis(Map<String,Object> request) {
        List<Map<String,Object>> rows=rows(request.get("data"));
        String groupColumn=required(request,"groupColumn"), valueColumn=required(request,"valueColumn");
        Map<String,List<Double>> groups=new LinkedHashMap<>();
        for(Map<String,Object> row:rows) {
            Object raw=row.get(valueColumn), group=row.get(groupColumn);
            if(group==null || !(raw instanceof Number n) || !Double.isFinite(n.doubleValue())) continue;
            groups.computeIfAbsent(String.valueOf(group),k->new ArrayList<>()).add(n.doubleValue());
        }
        if(groups.size()<2) throw new IllegalArgumentException("At least two groups are required");
        for(var e:groups.entrySet()) if(e.getValue().size()<2)
            throw new IllegalArgumentException("Each group needs at least two observations: "+e.getKey());
        List<Double> all=new ArrayList<>(); groups.values().forEach(all::addAll);
        List<Double> allRanks=ranks(all); int offset=0; double h=0;
        List<Map<String,Object>> summary=new ArrayList<>();
        for(var e:groups.entrySet()) {
            double rankSum=0; for(int i=0;i<e.getValue().size();i++) rankSum+=allRanks.get(offset++);
            int size=e.getValue().size(); h+=rankSum*rankSum/size;
            summary.add(Map.of("name",e.getKey(),"n",size,"median",median(e.getValue()),"meanRank",rankSum/size));
        }
        double n=all.size(); h=12*h/(n*(n+1))-3*(n+1);
        Map<Double,Integer> ties=new HashMap<>(); for(double v:all) ties.merge(v,1,Integer::sum);
        double tieSum=ties.values().stream().filter(t->t>1).mapToDouble(t->(double)t*t*t-t).sum();
        double correction=1-tieSum/(n*n*n-n);
        if(correction<=0) throw new IllegalArgumentException("All observations are tied; Kruskal-Wallis is undefined");
        h=Math.max(0,h/correction); int df=groups.size()-1;
        Map<String,Object> out=new LinkedHashMap<>();
        out.put("method","KRUSKAL_WALLIS_CHI_SQUARE_APPROXIMATION");
        out.put("groups",summary); out.put("sampleSize",all.size());
        out.put("hStatistic",h); out.put("degreesOfFreedom",df); out.put("pValue",gammaQ(df/2.0,h/2.0));
        out.put("epsilonSquared",Math.max(0,(h-groups.size()+1.0)/(n-groups.size())));
        out.put("notes",List.of("P-value uses the chi-square approximation; small samples may require exact or permutation inference.",
                "A significant omnibus result does not identify which groups differ; use multiplicity-adjusted post-hoc comparisons.",
                "Interpret as a location difference only when group distribution shapes are reasonably comparable."));
        return out;
    }

    public Map<String,Object> oneWayAnova(Map<String,Object> request) {
        List<Map<String,Object>> rows=rows(request.get("data"));
        String groupColumn=required(request,"groupColumn"), valueColumn=required(request,"valueColumn");
        Map<String,List<Double>> groups=new LinkedHashMap<>();
        for(Map<String,Object> row:rows) {
            Object raw=row.get(valueColumn), group=row.get(groupColumn);
            if(group==null || !(raw instanceof Number n) || !Double.isFinite(n.doubleValue())) continue;
            groups.computeIfAbsent(String.valueOf(group),k->new ArrayList<>()).add(n.doubleValue());
        }
        if(groups.size()<2) throw new IllegalArgumentException("At least two groups are required");
        for(var e:groups.entrySet()) if(e.getValue().size()<2)
            throw new IllegalArgumentException("Each group needs at least two observations: "+e.getKey());
        List<Double> all=new ArrayList<>(); groups.values().forEach(all::addAll);
        double grand=mean(all), ssBetween=0, ssWithin=0;
        List<Map<String,Object>> summary=new ArrayList<>();
        for(var e:groups.entrySet()) {
            List<Double> values=e.getValue(); double m=mean(values), variance=variance(values);
            ssBetween+=values.size()*(m-grand)*(m-grand);
            for(double v:values) ssWithin+=(v-m)*(v-m);
            summary.add(Map.of("name",e.getKey(),"n",values.size(),"mean",m,"standardDeviation",Math.sqrt(variance)));
        }
        int dfBetween=groups.size()-1, dfWithin=all.size()-groups.size();
        double msBetween=ssBetween/dfBetween, msWithin=ssWithin/dfWithin;
        if(msWithin<=0) throw new IllegalArgumentException("Within-group variance is zero; F statistic is undefined");
        double f=msBetween/msWithin;
        double p=regularizedBeta(dfWithin/(dfWithin+dfBetween*f),dfWithin/2.0,dfBetween/2.0);
        double total=ssBetween+ssWithin;
        Map<String,Object> out=new LinkedHashMap<>();
        out.put("method","ONE_WAY_CLASSICAL_ANOVA");
        out.put("groups",summary); out.put("sampleSize",all.size());
        out.put("betweenGroupSumSquares",ssBetween); out.put("withinGroupSumSquares",ssWithin);
        out.put("fStatistic",f); out.put("dfBetween",dfBetween); out.put("dfWithin",dfWithin);
        out.put("pValue",p); out.put("etaSquared",total==0?0:ssBetween/total);
        out.put("omegaSquared",Math.max(0,(ssBetween-dfBetween*msWithin)/(total+msWithin)));
        out.put("notes",List.of("Classical one-way ANOVA assumes independent observations, approximately normal residuals and equal group variances.",
                "This endpoint does not automatically select Welch ANOVA when homoscedasticity is violated.",
                "A significant omnibus result requires multiplicity-adjusted post-hoc tests to locate differences."));
        return out;
    }

    public Map<String,Object> detectTimeSeriesAnomalies(Map<String,Object> request) {
        Object raw=request.get("points");
        if(!(raw instanceof List<?> points) || points.size()<8 || points.size()>MAX_ROWS)
            throw new IllegalArgumentException("points must contain 8..100000 observations");
        int window=(int)number(request.get("window"),24);
        double threshold=number(request.get("threshold"),3.5);
        if(window<5 || window>500 || !(threshold>=1.5 && threshold<=12))
            throw new IllegalArgumentException("window must be 5..500 and threshold must be in [1.5,12]");
        List<Double> values=new ArrayList<>(); List<Object> timestamps=new ArrayList<>();
        for(Object item:points) {
            if(!(item instanceof Map<?,?> point) || !(point.get("value") instanceof Number n)
                    || !Double.isFinite(n.doubleValue()) || point.get("timestamp")==null)
                throw new IllegalArgumentException("Each point needs a timestamp and finite numeric value");
            values.add(n.doubleValue()); timestamps.add(point.get("timestamp"));
        }
        List<Map<String,Object>> anomalies=new ArrayList<>();
        int warmup=Math.max(5,Math.min(window,window/2));
        for(int i=warmup;i<values.size();i++) {
            List<Double> history=new ArrayList<>(values.subList(Math.max(0,i-window),i));
            double center=median(history);
            List<Double> deviations=history.stream().map(v->Math.abs(v-center)).toList();
            double mad=median(deviations), scale=1.4826*mad;
            double score=scale>1e-12?Math.abs(values.get(i)-center)/scale:
                    (Math.abs(values.get(i)-center)<1e-12?0:1_000_000);
            if(score>=threshold) {
                Map<String,Object> anomaly=new LinkedHashMap<>();
                anomaly.put("index",i); anomaly.put("timestamp",timestamps.get(i));
                anomaly.put("observed",values.get(i)); anomaly.put("rollingMedian",center);
                anomaly.put("robustZScore",score); anomaly.put("direction",values.get(i)>=center?"HIGH":"LOW");
                anomalies.add(anomaly);
            }
        }
        Map<String,Object> out=new LinkedHashMap<>();
        out.put("method","ROLLING_MEDIAN_MAD_ROBUST_Z_SCORE");
        out.put("inputPoints",values.size()); out.put("evaluatedPoints",values.size()-warmup);
        out.put("window",window); out.put("threshold",threshold);
        out.put("anomalyCount",anomalies.size()); out.put("anomalies",anomalies);
        out.put("notes",List.of("Each point is compared only with preceding observations; the current point is excluded from its baseline.",
                "This is a univariate robust baseline and does not model seasonality, trend, holidays or cross-series relationships.",
                "Input points must already be ordered chronologically; tune the window and threshold using a held-out period."));
        return out;
    }

    public Map<String,Object> estimateCausalEffect(Map<String,Object> request) {
        List<Map<String,Object>> rows=rows(request.get("data"));
        if(rows.size()>20_000) throw new IllegalArgumentException("Causal propensity analysis is limited to 20000 rows");
        String treatment=required(request,"treatmentColumn"), outcome=required(request,"outcomeColumn");
        String treatedValue=required(request,"treatedValue");
        Object rawCov=request.get("covariates");
        if(!(rawCov instanceof List<?> covList) || covList.size()>10)
            throw new IllegalArgumentException("covariates must be an array with at most 10 numeric columns");
        List<String> covariates=covList.stream().map(String::valueOf).distinct().toList();
        List<double[]> xRows=new ArrayList<>(); List<Double> y=new ArrayList<>(); List<Integer> t=new ArrayList<>();
        for(Map<String,Object> row:rows) {
            Object tr=row.get(treatment), out=row.get(outcome);
            if(tr==null || !(out instanceof Number on) || !Double.isFinite(on.doubleValue())) continue;
            double[] x=new double[covariates.size()]; boolean valid=true;
            for(int j=0;j<covariates.size();j++) {
                Object v=row.get(covariates.get(j));
                if(!(v instanceof Number nn) || !Double.isFinite(nn.doubleValue())) { valid=false; break; }
                x[j]=nn.doubleValue();
            }
            if(valid) { xRows.add(x); y.add(on.doubleValue()); t.add(Objects.equals(String.valueOf(tr),treatedValue)?1:0); }
        }
        int n=xRows.size(), treated=(int)t.stream().filter(v->v==1).count(), control=n-treated;
        if(n<20 || treated<5 || control<5) throw new IllegalArgumentException("Need at least 20 complete rows and at least 5 observations in each treatment arm");
        int p=covariates.size();
        double[] means=new double[p], scales=new double[p];
        for(int j=0;j<p;j++) {
            final int col=j; means[j]=xRows.stream().mapToDouble(r->r[col]).average().orElse(0);
            double sum=0; for(double[] x:xRows) sum+=(x[j]-means[j])*(x[j]-means[j]);
            scales[j]=Math.sqrt(sum/n); if(scales[j]<1e-12) scales[j]=1;
        }
        double[][] design=new double[n][p+1];
        for(int i=0;i<n;i++) { design[i][0]=1; for(int j=0;j<p;j++) design[i][j+1]=(xRows.get(i)[j]-means[j])/scales[j]; }
        double[] beta=new double[p+1];
        // Regularized batch gradient descent for a logistic propensity model.
        for(int iteration=0;iteration<600;iteration++) {
            double[] gradient=new double[p+1];
            for(int i=0;i<n;i++) {
                double linear=0; for(int j=0;j<beta.length;j++) linear+=beta[j]*design[i][j];
                double error=sigmoid(linear)-t.get(i);
                for(int j=0;j<beta.length;j++) gradient[j]+=error*design[i][j];
            }
            for(int j=0;j<beta.length;j++) {
                gradient[j]=gradient[j]/n+(j==0?0:1e-3*beta[j]);
                beta[j]-=0.1*gradient[j];
            }
        }
        double[] propensity=new double[n], weights=new double[n];
        double sumT=0,sumC=0,weightT=0,weightC=0,weightSqT=0,weightSqC=0;
        int clipped=0;
        for(int i=0;i<n;i++) {
            double linear=0; for(int j=0;j<beta.length;j++) linear+=beta[j]*design[i][j];
            double ps=sigmoid(linear); if(ps<0.01 || ps>0.99) clipped++;
            ps=Math.max(0.01,Math.min(0.99,ps)); propensity[i]=ps;
            weights[i]=t.get(i)==1?1/ps:1/(1-ps);
            if(t.get(i)==1) { sumT+=weights[i]*y.get(i); weightT+=weights[i]; weightSqT+=weights[i]*weights[i]; }
            else { sumC+=weights[i]*y.get(i); weightC+=weights[i]; weightSqC+=weights[i]*weights[i]; }
        }
        double meanT=sumT/weightT, meanC=sumC/weightC;
        List<Map<String,Object>> balance=new ArrayList<>();
        for(int j=0;j<p;j++) {
            double rawT=0,rawC=0,wtT=0,wtC=0,wT=0,wC=0;
            for(int i=0;i<n;i++) {
                if(t.get(i)==1) { rawT+=xRows.get(i)[j]; wtT+=weights[i]*xRows.get(i)[j]; wT+=weights[i]; }
                else { rawC+=xRows.get(i)[j]; wtC+=weights[i]*xRows.get(i)[j]; wC+=weights[i]; }
            }
            double smdBefore=(rawT/treated-rawC/control)/scales[j];
            double smdAfter=(wtT/wT-wtC/wC)/scales[j];
            balance.add(Map.of("covariate",covariates.get(j),"standardizedMeanDifferenceBefore",smdBefore,
                    "standardizedMeanDifferenceAfter",smdAfter));
        }
        Map<String,Object> out=new LinkedHashMap<>();
        out.put("method","PROPENSITY_SCORE_INVERSE_PROBABILITY_WEIGHTED_ATE");
        out.put("estimand","AVERAGE_TREATMENT_EFFECT");
        out.put("completeCaseSampleSize",n); out.put("treatedN",treated); out.put("controlN",control);
        out.put("treatedWeightedMean",meanT); out.put("controlWeightedMean",meanC);
        out.put("estimatedAverageTreatmentEffect",meanT-meanC);
        out.put("effectiveSampleSize",Map.of("treated",weightT*weightT/weightSqT,"control",weightC*weightC/weightSqC));
        out.put("propensityClippedCount",clipped); out.put("covariateBalance",balance);
        out.put("notes",List.of("Observational inverse-probability weighting with a regularized logistic propensity model; no confidence interval is calculated.",
                "Causal interpretation requires consistency, positivity, correct propensity specification and no unmeasured confounding.",
                "Inspect post-weighting standardized mean differences; values above 0.1 commonly indicate material residual imbalance.",
                "Propensity clipping improves numerical stability but changes the effective weighting and does not repair lack of overlap."));
        return out;
    }

    public Map<String,Object> designContinuousExperiment(Map<String,Object> request) {
        double effect=number(request.get("standardizedEffect"),Double.NaN);
        double alpha=number(request.get("alpha"),0.05), power=number(request.get("power"),0.8);
        double ratio=number(request.get("allocationRatio"),1), attrition=number(request.get("attritionRate"),0);
        if(!(effect>0) || !(alpha>0.001 && alpha<0.2) || !(power>0.5 && power<0.999)
                || !(ratio>=0.1 && ratio<=10) || !(attrition>=0 && attrition<0.5))
            throw new IllegalArgumentException("Require standardizedEffect>0, alpha in (0.001,0.2), power in (0.5,0.999), allocationRatio in [0.1,10], attritionRate in [0,0.5)");
        double zAlpha=inverseNormal(1-alpha/2), zPower=inverseNormal(power);
        double control=(zAlpha+zPower)*(zAlpha+zPower)*(1+1/ratio)/(effect*effect);
        int controlN=(int)Math.ceil(control/(1-attrition));
        int treatedN=(int)Math.ceil(control*ratio/(1-attrition));
        Map<String,Object> out=new LinkedHashMap<>();
        out.put("method","TWO_ARM_CONTINUOUS_OUTCOME_NORMAL_APPROXIMATION");
        out.put("standardizedEffectCohensD",effect); out.put("alpha",alpha); out.put("targetPower",power);
        out.put("allocationRatioTreatedToControl",ratio); out.put("attritionRate",attrition);
        out.put("requiredControlN",controlN); out.put("requiredTreatedN",treatedN);
        out.put("totalRequiredN",controlN+treatedN);
        out.put("notes",List.of("Normal approximation for two independent means, two-sided test and known/large-sample variance planning.",
                "The standardized effect must come from domain knowledge or defensible pilot evidence, not the observed effect from the same confirmatory sample.",
                "Does not account for clustering, repeated measures, covariate adjustment, multiple primary outcomes or interim looks; inflate further when applicable."));
        return out;
    }

    private static List<Map<String,Object>> rows(Object raw) {
        if(!(raw instanceof List<?> list) || list.isEmpty() || list.size()>MAX_ROWS)
            throw new IllegalArgumentException("data must contain 1.."+MAX_ROWS+" rows");
        List<Map<String,Object>> result=new ArrayList<>();
        for(Object item:list) {
            if(!(item instanceof Map<?,?> map)) throw new IllegalArgumentException("Every data row must be an object");
            Map<String,Object> row=new LinkedHashMap<>(); map.forEach((k,v)->row.put(String.valueOf(k),v)); result.add(row);
        }
        return result;
    }
    private static String required(Map<String,Object> r,String key) {
        Object v=r.get(key); if(v==null || String.valueOf(v).isBlank()) throw new IllegalArgumentException(key+" is required");
        return String.valueOf(v);
    }
    private static double number(Object v,double fallback) { return v instanceof Number n?n.doubleValue():fallback; }
    private static double mean(List<Double> v) { return v.stream().mapToDouble(Double::doubleValue).average().orElse(0); }
    private static double variance(List<Double> v) { double m=mean(v); return v.size()<2?0:v.stream().mapToDouble(x->(x-m)*(x-m)).sum()/(v.size()-1); }
    private static double median(List<Double> values) { List<Double> s=new ArrayList<>(values); s.sort(Double::compare); int n=s.size(); return n%2==1?s.get(n/2):(s.get(n/2-1)+s.get(n/2))/2; }
    private static List<Double> ranks(List<Double> values) {
        List<Integer> order=new ArrayList<>(); for(int i=0;i<values.size();i++) order.add(i);
        order.sort(Comparator.comparingDouble(values::get)); double[] out=new double[values.size()];
        for(int i=0;i<order.size();) { int j=i+1; while(j<order.size() && Double.compare(values.get(order.get(i)),values.get(order.get(j)))==0) j++;
            double rank=(i+1+j)/2.0; for(int k=i;k<j;k++) out[order.get(k)]=rank; i=j; }
        List<Double> result=new ArrayList<>(); for(double v:out) result.add(v); return result;
    }
    private static double sigmoid(double x) { if(x>=0) { double e=Math.exp(-Math.min(x,35)); return 1/(1+e); } double e=Math.exp(Math.max(x,-35)); return e/(1+e); }
    private static double normalCdf(double x) {
        double z=Math.abs(x)/Math.sqrt(2), t=1/(1+0.3275911*z);
        double erf=1-(((((1.061405429*t-1.453152027)*t)+1.421413741)*t-0.284496736)*t+0.254829592)*t*Math.exp(-z*z);
        return 0.5*(1+Math.copySign(erf,x));
    }
    private static double inverseNormal(double p) {
        double[] a={-3.969683028665376e1,2.209460984245205e2,-2.759285104469687e2,1.383577518672690e2,-3.066479806614716e1,2.506628277459239};
        double[] b={-5.447609879822406e1,1.615858368580409e2,-1.556989798598866e2,6.680131188771972e1,-1.328068155288572e1};
        double[] c={-7.784894002430293e-3,-3.223964580411365e-1,-2.400758277161838,-2.549732539343734,4.374664141464968,2.938163982698783};
        double[] d={7.784695709041462e-3,3.224671290700398e-1,2.445134137142996,3.754408661907416};
        double q,r;
        if(p<0.02425) { q=Math.sqrt(-2*Math.log(p)); return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5])/((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1); }
        if(p>0.97575) { q=Math.sqrt(-2*Math.log(1-p)); return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5])/((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1); }
        q=p-0.5; r=q*q; return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q/(((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1);
    }
    private static double gammaQ(double a,double x) {
        if(x<=0)return 1;
        if(x<a+1) { double sum=1/a,term=sum,ap=a; for(int i=1;i<=500;i++){term*=x/++ap;sum+=term;if(Math.abs(term)<Math.abs(sum)*1e-14)break;} return 1-sum*Math.exp(-x+a*Math.log(x)-logGamma(a)); }
        double b=x+1-a,c=1e300,d=1/b,h=d;
        for(int i=1;i<=500;i++){double an=-i*(i-a);b+=2;d=an*d+b;if(Math.abs(d)<1e-300)d=1e-300;c=b+an/c;if(Math.abs(c)<1e-300)c=1e-300;d=1/d;double delta=d*c;h*=delta;if(Math.abs(delta-1)<1e-14)break;}
        return Math.exp(-x+a*Math.log(x)-logGamma(a))*h;
    }
    private static double regularizedBeta(double x,double a,double b) {
        if(x<=0)return 0;if(x>=1)return 1;
        double bt=Math.exp(logGamma(a+b)-logGamma(a)-logGamma(b)+a*Math.log(x)+b*Math.log1p(-x));
        if(x<(a+1)/(a+b+2))return bt*betaFraction(x,a,b)/a;
        return 1-bt*betaFraction(1-x,b,a)/b;
    }
    private static double betaFraction(double x,double a,double b) {
        double qab=a+b,qap=a+1,qam=a-1,c=1,d=1-qab*x/qap;if(Math.abs(d)<1e-300)d=1e-300;d=1/d;double h=d;
        for(int m=1;m<=300;m++){int m2=2*m;double aa=m*(b-m)*x/((qam+m2)*(a+m2));d=1+aa*d;if(Math.abs(d)<1e-300)d=1e-300;c=1+aa/c;if(Math.abs(c)<1e-300)c=1e-300;d=1/d;h*=d*c;
            aa=-(a+m)*(qab+m)*x/((a+m2)*(qap+m2));d=1+aa*d;if(Math.abs(d)<1e-300)d=1e-300;c=1+aa/c;if(Math.abs(c)<1e-300)c=1e-300;d=1/d;double delta=d*c;h*=delta;if(Math.abs(delta-1)<3e-14)break;}
        return h;
    }
    private static double logGamma(double z) {
        double[] p={676.5203681218851,-1259.1392167224028,771.3234287776531,-176.6150291621406,12.507343278686905,-0.13857109526572012,9.984369578019572e-6,1.5056327351493116e-7};
        if(z<0.5)return Math.log(Math.PI)-Math.log(Math.sin(Math.PI*z))-logGamma(1-z);
        z-=1;double x=0.99999999999980993;for(int i=0;i<p.length;i++)x+=p[i]/(z+i+1);double t=z+p.length-0.5;
        return 0.5*Math.log(2*Math.PI)+(z+0.5)*Math.log(t)-t+Math.log(x);
    }
}
