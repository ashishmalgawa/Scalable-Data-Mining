import com.sun.org.apache.bcel.internal.generic.ARRAYLENGTH;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import java.util.*;

import static java.lang.Math.ceil;
import static java.lang.Math.floor;

public class KPercentile {
    public static void main(String[] args) {
        // Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("K Percentile");
        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile( "/home/ashish/IdeaProjects/KPercentile/input/input_list.txt" );
        Broadcast<Long> n = sc.broadcast(input.count());
        Broadcast<Double> hasher=sc.broadcast(( floor(input.flatMapToDouble(new DoubleFlatMapFunction<String>() {
            @Override
            public Iterable<Double> call(String s) throws Exception {
                Double d = Double.parseDouble(s);
                ArrayList<Double> a = new ArrayList<>();
                a.add(d);
                return a;
            }
        }).mean() / 50)));//To calculate 1 Percent of the sum
        JavaPairRDD<Integer,Tuple2<Integer,Integer>> mappedOne=input.flatMapToPair(new PairFlatMapFunction<String, Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Iterable<Tuple2<Integer, Tuple2<Integer, Integer>>> call(String s) throws Exception {
                Integer num=Integer.parseInt(s);
                int key=num/hasher.getValue().intValue();
                ArrayList<Tuple2<Integer,Tuple2<Integer,Integer>>> Pairs= new ArrayList<>();
                Pairs.add(new Tuple2<Integer,Tuple2<Integer,Integer>>(key,new Tuple2<Integer,Integer>(num,0)));
                key++;
                for (;key<=100;key++)
                    Pairs.add(new Tuple2<Integer,Tuple2<Integer,Integer>>(key,new Tuple2<Integer,Integer>(-1,1)));

                return Pairs;
            }
        });
        JavaPairRDD<Integer,Tuple2<Iterable<Integer>,Integer>> groupedOne=mappedOne.groupByKey().mapValues(new Function<Iterable<Tuple2<Integer, Integer>>, Tuple2<Iterable<Integer>, Integer>>() {
            @Override
            public Tuple2<Iterable<Integer>, Integer> call(Iterable<Tuple2<Integer, Integer>> tuple2s) throws Exception {
                int count=0;
                ArrayList<Integer> list=new ArrayList<>();
                for (Tuple2<Integer, Integer> tuple2 : tuple2s) {
                    count+=tuple2._2;
                    if (tuple2._1!=-1)
                    list.add(tuple2._1);
                }
                return new Tuple2<Iterable<Integer>,Integer>(list,count);
            }
        });
        JavaPairRDD<Integer,Double> mappedTwo=groupedOne.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Tuple2<Iterable<Integer>, Integer>>, Integer, Double>() {
            @Override
            public Iterable<Tuple2<Integer, Double>> call(Tuple2<Integer, Tuple2<Iterable<Integer>, Integer>> integerTuple2Tuple2) throws Exception {
                int count = 0;
                for (Integer integer : integerTuple2Tuple2._2._1) {
                    count++;
                }
                float k[] = new float[6];
                k[0] = (float) (n.getValue() * .25);
                if (ceil(k[0]) == k[0])
                    k[1] = k[0] + 1;
                else {
                    k[0]= (float) ceil(k[0]);
                    k[1] = -1;
                }k[2] = (float) (n.getValue() * .5);
                if (ceil(k[2]) == k[2])
                    k[3] = k[2] + 1;
                else {
                    k[3]=(float) ceil(k[3]);
                    k[3] = -1;
                }k[4] = (float) (n.getValue() * .75);
                if (ceil(k[0]) == k[0])
                    k[5] = k[4] + 1;
                else {
                    k[5] = (float) ceil(k[5]);
                    k[5] = -1;
                }Double num;
                ArrayList<Tuple2<Integer,Double>> list=new ArrayList<>();
                for (int i = 0; i < 6; i++) {
                    if (k[i] < count + integerTuple2Tuple2._2._2 && k[i] > integerTuple2Tuple2._2._2 && k[i] != -1) {
                        num = (double) Select.quickselect(integerTuple2Tuple2._2._1, (int)k[i]-integerTuple2Tuple2._2._2);
                        Integer key= (int) floor(k[i]/2);
                        list.add(new Tuple2<Integer,Double>(key,  num));
                }
                }
                return list;

            }
            });
        JavaPairRDD<Integer,Double> finalMost=mappedTwo.reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2) throws Exception {
                return (aDouble+aDouble2)/2;
            }
        });
        JavaPairRDD<Integer,Double> mappedThree=finalMost.mapToPair(new PairFunction<Tuple2<Integer, Double>, Integer, Double>() {
            @Override
            public Tuple2<Integer, Double> call(Tuple2<Integer, Double> integerDoubleTuple2) throws Exception {

                return new Tuple2<Integer,Double>(1,integerDoubleTuple2._2);
            }
        });
        JavaPairRDD<Integer,Iterable<Double>> finalGrouped=mappedThree.groupByKey();
        JavaRDD<String> finalAnswer=finalGrouped.map(new Function<Tuple2<Integer, Iterable<Double>>, String>() {
            @Override
            public String call(Tuple2<Integer, Iterable<Double>> integerIterableTuple2) throws Exception {
                String s="";
                ArrayList<Double> list=new ArrayList<>();

                for (Double aDouble : integerIterableTuple2._2) {
                    list.add(aDouble);
                }
                Collections.sort(list);
                for (Double aDouble : list) {
                    s=s+aDouble+'\t';
                }
                return s;
            }
        });
        finalAnswer.saveAsTextFile("output");
    }
}
class Select{

    public static int quickselect(Iterable<Integer> G, int k) {
        int count=0;
        for (Integer integer : G) {
           count++;
        }
        int g[]=new int[count];
        count=0;
        for (Integer integer : G) {
            g[count++]=integer;
        }
        return  quickselect(g, 0, g.length - 1, k - 1);
    }

    private static int quickselect(int[] G, int first, int last, int k) {
        if (first <= last) {
            int pivot = partition(G, first, last);
            if (pivot == k) {
                return G[k];
            }
            if (pivot > k) {
                return quickselect(G, first, pivot - 1, k);
            }
            return quickselect(G, pivot + 1, last, k);
        }
        System.out.println("returning"+Integer.MIN_VALUE);
        return Integer.MIN_VALUE;
    }
    private static int  partition(int[] G, int first, int last) {
        int pivot = first + new Random().nextInt(last - first + 1);
        swap(G, last, pivot);
        for (int i = first; i < last; i++) {
            if (G[i] > G[last]) {
                swap(G, i, first);
                first++;
            }
        }
        swap(G, first, last);
        return first;
    }

    private static void swap(int[] G, int x, int y) {
        int tmp = G[x];
        G[x] = G[y];
        G[y] = tmp;
    }
}
