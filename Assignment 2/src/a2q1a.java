import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import java.util.*;
public class assignment2 {
    public static void main( String args[] )
    {
        // Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Matrix Multiplication");
        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile( "/home/ashish/IdeaProjects/assignment2/input/input_matrices.txt" );
        long communicationCost=input.count();
        JavaPairRDD<Tuple2<Character,Integer>,Tuple2<Integer,Integer>> mapped1=input.flatMapToPair(new PairFlatMapFunction<String, Tuple2<Character, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Iterable<Tuple2<Tuple2<Character, Integer>, Tuple2<Integer, Integer>>> call(String s) throws Exception {
                String [] items=s.split(",");
                ArrayList<Tuple2<Tuple2<Character,Integer>,Tuple2<Integer,Integer>>> list=new ArrayList<>();
                if(items[0].equals("A")) {
                    list.add(new Tuple2<Tuple2<Character, Integer>, Tuple2<Integer, Integer>>(new Tuple2<Character, Integer>('A', Integer.parseInt(items[1])),
                            new Tuple2<Integer, Integer>(Integer.parseInt(items[2]), Integer.parseInt(items[3]))));
                }else{
                    list.add(new Tuple2<Tuple2<Character, Integer>, Tuple2<Integer, Integer>>(new Tuple2<Character, Integer>('B', Integer.parseInt(items[2])),
                            new Tuple2<Integer, Integer>(Integer.parseInt(items[1]), Integer.parseInt(items[3]))));
                }
                return list;
            }
        });
        communicationCost+=mapped1.count();
        JavaPairRDD<Tuple2<Character,Integer>,Iterable<Integer>> reducedOne
                = mapped1.groupByKey().mapValues(new Function<Iterable<Tuple2<Integer, Integer>>, Iterable<Integer>>() {
            @Override
            public Iterable<Integer> call(Iterable<Tuple2<Integer, Integer>> tuple2s) throws Exception {
                List<MySort> list= new ArrayList<MySort>();
                tuple2s.forEach(k->{
                    MySort s=new MySort();
                    s.index=k._1;
                    s.value=k._2;
                    list.add(s);
                });
                Collections.sort(list);
                List<Integer> ids=new ArrayList<>();
                int x=0;
                for(MySort s:list){
                    ids.add(s.value);
                }
                return ids;
            }
        });
        communicationCost+=reducedOne.count();
        JavaPairRDD<Tuple2<Integer,Integer>,Iterable<Integer>> mappedTwo=reducedOne.flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<Character, Integer>, Iterable<Integer>>, Tuple2<Integer, Integer>, Iterable<Integer>>() {
            @Override
            public Iterable<Tuple2<Tuple2<Integer, Integer>, Iterable<Integer>>> call(Tuple2<Tuple2<Character, Integer>, Iterable<Integer>> tuple2IterableTuple2) throws Exception {
                ArrayList<Tuple2<Tuple2<Integer,Integer>,Iterable<Integer>>> list=new ArrayList<>();
                if(tuple2IterableTuple2._1._1=='A'){
                    int i = 0;
                    Iterator iterator=tuple2IterableTuple2._2.iterator();
                    while(iterator.hasNext()){
                        list.add(new Tuple2<Tuple2<Integer,Integer>,Iterable<Integer>>
                                (new Tuple2<Integer,Integer>(tuple2IterableTuple2._1._2,i),tuple2IterableTuple2._2));
                    i++;
                    iterator.next();
                    }
                }else{
                    int i = 0;
                    Iterator iterator=tuple2IterableTuple2._2.iterator();
                    while(iterator.hasNext()){
                        list.add(new Tuple2<Tuple2<Integer, Integer>, Iterable<Integer>>
                                (new Tuple2<Integer, Integer>(i, tuple2IterableTuple2._1._2), tuple2IterableTuple2._2));
                        i++;
                        iterator.next();
                    }
                }
                return list;
            }
        });
        communicationCost+=mappedTwo.count();
        JavaPairRDD<Tuple2<Integer,Integer>,Integer> reducedTwo= mappedTwo.reduceByKey(new Function2<Iterable<Integer>, Iterable<Integer>, Iterable<Integer>>() {
            @Override
            public Iterable<Integer> call(Iterable<Integer> integers, Iterable<Integer> integers2) throws Exception {
                Iterator iterator=integers.iterator();
                Iterator iterator2=integers2.iterator();
                List<Integer> list=new ArrayList<>();
                while(iterator.hasNext()&&iterator2.hasNext()){
                    Integer a=(Integer)iterator.next();
                    Integer b=(Integer)iterator2.next();
                    list.add(a*b);
                }
                return list;
            }
        }).mapValues(new Function<Iterable<Integer>, Integer>() {
            @Override
            public Integer call(Iterable<Integer> integers) throws Exception {
                Iterator iterator=integers.iterator();
                List<Integer> list=new ArrayList<>();
                int sum=0;
                while(iterator.hasNext()){
                    Integer a=(Integer)iterator.next();
                    sum+=a;
                }
                return sum;
            }
        });
        communicationCost+=reducedTwo.count();
        JavaPairRDD<Integer, String> finalAnswer=reducedTwo.mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<Tuple2<Integer, Integer>, Integer> tuple2IntegerTuple2) throws Exception {
                int key = tuple2IntegerTuple2._1._1*100+tuple2IntegerTuple2._1._2;
                String s=tuple2IntegerTuple2._1._1+","+tuple2IntegerTuple2._1._2+","+tuple2IntegerTuple2._2;
                return new Tuple2<>(key,s);
            }
        }).sortByKey();
        System.out.println(communicationCost);
        finalAnswer.values().saveAsTextFile("output");
    }
}
class MySort implements Comparable{
    Integer index;
    Integer value;
    @Override
    public int compareTo(Object o) {
        return this.index-((MySort)o).index;
    }
}
