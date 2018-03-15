import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import java.util.*;
public class FriendFinder {
    public static void main( String args[] )
    {
        // Define a configuration to use to interact with Spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Friend Recommendation Program");
        // Create a Java version of the Spark Context from the configuration
        JavaSparkContext sc = new JavaSparkContext(conf);
       JavaRDD<String> input = sc.textFile( "/home/ashish/IdeaProjects/FriendRecommendations/input/FriendData.txt" );
       JavaPairRDD<Tuple2<String,String>,Integer> mapped1= input.flatMapToPair ( new  PairFlatMapFunction <String, Tuple2<String,String>, Integer> () {
            @Override
            public Iterable<Tuple2<Tuple2<String,String>, Integer>> call(String s) throws Exception {
                String[] words = s.split("\t");
                Integer one = new Integer(1);
                Integer zero = new Integer(0);
                if(words.length>1) {
                    String[] words2 = words[1].split(",");
                    ArrayList<Tuple2<Tuple2<String, String>, Integer>> a = new ArrayList<Tuple2<Tuple2<String, String>, Integer>>();
                    for (int i = 0; i < words2.length; i++) {
                        Tuple2<String, String> temp = new Tuple2<String, String>(words2[i], words[0]);
                        a.add(new Tuple2<Tuple2<String, String>, Integer>(temp, zero));
                    }
                    for (int i = 0; i < words2.length; i++) {
                        for (int j = 0; j < words2.length; j++) {
                            if (j != i) {
                                Tuple2<String, String> temp = new Tuple2<String, String>(words2[i], words2[j]);
                                a.add(new Tuple2<Tuple2<String, String>, Integer>(temp, one));
                            }
                        }
                    }
                    return a;
                }
                else{
                    ArrayList<Tuple2<Tuple2<String, String>, Integer>> a = new ArrayList<Tuple2<Tuple2<String, String>, Integer>>();
                    Tuple2<String, String> temp = new Tuple2<String, String>("", words[0]);
                    a.add(new Tuple2<Tuple2<String, String>, Integer>(temp, zero));
                    return a;
                }
            }
        });
        JavaPairRDD<Tuple2<String,String>,Integer> reduced1 = mapped1.reduceByKey(
                new Function2<Integer, Integer, Integer>(){
                    public Integer call(Integer x, Integer y){
                        if(y==0||x==0)
                            return 0;
                        else
                        return x + y;}
                } );
        JavaPairRDD<Tuple2<String,String>,Integer> filtered=reduced1.filter(new Function<Tuple2<Tuple2<String, String>, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2 ) throws Exception {
                if (tuple2IntegerTuple2._2>0)
                    return true;
                else
                    return false;
            }
        });
       JavaPairRDD<String, Tuple2<String, Integer>> mappedTwo = filtered.flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<String,String>,Integer>, String, Tuple2<String,Integer>>() {
            @Override
            public Iterable<Tuple2<String, Tuple2<String, Integer>>> call(Tuple2<Tuple2<String, String>, Integer> tuple2IntegerTuple2) throws Exception {
                ArrayList<Tuple2<String,Tuple2<String,Integer>>> a=new ArrayList<Tuple2<String,Tuple2<String,Integer>>>();
                a.add(new Tuple2<String,Tuple2<String,Integer>>(tuple2IntegerTuple2._1._1,new Tuple2<String,Integer>(tuple2IntegerTuple2._1._2,tuple2IntegerTuple2._2)));
                return a;
            }
        });
        JavaPairRDD<String,List<String>> grouped=mappedTwo.groupByKey().mapValues(new Function<Iterable<Tuple2<String, Integer>>, List<String>>() {
            @Override
            public List<String> call(Iterable<Tuple2<String, Integer>> tuple2s) throws Exception {
                List <MySort> temp=new ArrayList<MySort>();
                tuple2s.forEach(k->{
                    MySort s=new MySort();
                    s.userid=k._1;
                    s.count=k._2;
                    temp.add(s);
                });
                Collections.sort(temp);
                List<String> ids=new ArrayList<>();
                int x=0;
                for(MySort s:temp){
                    if(x>=10)
                        break;
                    ids.add(s.userid);
                    x++;
                }
                return ids;
            }
        });
        JavaRDD<String> printRDD = grouped.map(new Function<Tuple2<String, List<String>>, String>() {
            @Override
            public String call(Tuple2<String, List<String>> stringListTuple2) throws Exception {
                String output_line="";
                output_line=output_line.concat(stringListTuple2._1+"\t");
                int x=0;
                for (String Recommendation : stringListTuple2._2){
                    output_line=output_line.concat(Recommendation);
                    if(x<stringListTuple2._2.size()-1){
                        output_line=output_line.concat(",");
                    }
                    x++;
                }
                return output_line;
            }
        });
        printRDD.saveAsTextFile("output");
    }
}
class MySort implements Comparable{
    String userid;
    Integer count;
    @Override
    public int compareTo(Object o) {
        return ((MySort)o).count-this.count;
    }
}
