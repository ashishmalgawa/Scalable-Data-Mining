import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.feature.MinHashLSH;
import org.apache.spark.ml.feature.MinHashLSHModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import scala.Tuple2;
import java.io.PrintWriter;
import java.util.*;
import org.apache.spark.mllib.clustering.PowerIterationClustering;
import org.apache.spark.mllib.clustering.PowerIterationClusteringModel;
import scala.Tuple3;
import static org.apache.spark.sql.functions.col;
public class Assignment3 {
    public static void main(String[] args) {
        //--------------------------------------------------------------------//
        //Intialisation
        SparkConf conf=new SparkConf().setMaster("local").setAppName("as");
        JavaSparkContext sc=new JavaSparkContext(conf);
        SparkSession spark=SparkSession.builder().master("local").appName("app").getOrCreate();
        JavaRDD<String> input=sc.textFile("input/Data.txt");

        //------------------------------------------------------------------------//
        JavaPairRDD<String, Integer> mappedOne=input.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                Collection<Tuple2<String,Integer>> shingles=new ArrayList<>();
                if(!s.isEmpty() && s.charAt(0)!='\t') {
                    //checking if there is any data in given line
                    String str[] = s.split("\t");
                    Integer paraNo = Integer.parseInt(str[0]);
                    String para=str[1];
                    if(str[1].charAt(0)=='\"' || str[1].charAt(0)=='�')//Since data.txt contains two types of ' " '
                        para = str[1].substring(1, str[1].length() - 2);
                    Set<String> set=new HashSet<String>();
                    int i=0;
                    while(i+5<para.length()) {
                        set.add(para.substring(i, i + 5));
                        i++;
                    }
                    //getting unique shingles
                    Iterator setIterator=set.iterator();
                    while(setIterator.hasNext()) {
                        shingles.add(new Tuple2<String,Integer>((String) setIterator.next(), paraNo));
                    }
                }
                return shingles.iterator();
            }
        });

        JavaPairRDD<String, Iterable<Integer>> invertedIndex=mappedOne.groupByKey();

        JavaPairRDD<Tuple2<String,Iterable<Integer>>,Long> shingleHashed=invertedIndex.zipWithUniqueId();
        //This will map each shingle to an integer
        Broadcast<Integer> totalNumberOfShingles=sc.broadcast(shingleHashed.countByKey().size());
        //Since MinHashLSH needs array of shingle ids present in a document
        //This will first create doc,shingleid pair
        JavaPairRDD<Integer,Integer> docShinglePair=shingleHashed.flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<String, Iterable<Integer>>, Long>, Integer, Integer>() {
            @Override
            public Iterator<Tuple2<Integer, Integer>> call(Tuple2<Tuple2<String, Iterable<Integer>>, Long> tuple2LongTuple2) throws Exception {
                Collection<Tuple2<Integer,Integer>> list=new ArrayList<>();
                for (Integer integer : tuple2LongTuple2._1._2) {

                    list.add(new Tuple2<Integer,Integer>(integer,tuple2LongTuple2._2.intValue()));
                }
                return list.iterator();
            }
        });

        JavaPairRDD<Integer,Iterable<Integer>> groupedOne=docShinglePair.groupByKey();
        //now the data will be transformed in desired data structure
        JavaRDD<Row> minHashInput=groupedOne.map(new Function<Tuple2<Integer, Iterable<Integer>>, Row>() {
            @Override
            public Row call(Tuple2<Integer, Iterable<Integer>> integerIterableTuple2) throws Exception {
                int count=0;
                for (Integer integer : integerIterableTuple2._2) {
                    count++;
                }
                int [] a=new int[count];
                double[] b=new double[count];
                int i=0;
                for (Integer integer : integerIterableTuple2._2) {
                    a[i]=integer;
                    b[i]=1.0;
                    i++;
                }
                Row row=RowFactory.create(integerIterableTuple2._1,Vectors.sparse(totalNumberOfShingles.getValue().intValue(),a,b));
                return row;
            }
        });
        //schema for data structure
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> dfA = spark.createDataFrame(minHashInput, schema);
        //Applying min hash LSH
        MinHashLSH mh = new MinHashLSH()
                .setNumHashTables(20)
                .setInputCol("features")
                .setOutputCol("hashes");

        MinHashLSHModel model = mh.fit(dfA);

        model.transform(dfA).show();
        //calculating jaccardDistance between each pair since it is needed in Power Iteration Clustering
        Dataset<Row> distanceMatrix=model.approxSimilarityJoin(dfA, dfA, 1, "JaccardDistance")
                .select(col("datasetA.id").alias("idA"),
                        col("datasetB.id").alias("idB"),
                        col("JaccardDistance"));
        //Transforming into the desired data structure for PIC
        JavaRDD<Tuple3<Long, Long, Double>> similarities =distanceMatrix.javaRDD().map(new Function<Row, Tuple3<Long, Long, Double>>() {
            @Override
            public Tuple3<Long, Long, Double> call(Row row) throws Exception {
                Integer a=row.getInt(0);
                Integer b=row.getInt(1);
                Double c=1-row.getDouble(2);
                return new Tuple3<>(a.longValue(),b.longValue(),c);
            }
        });

        PowerIterationClustering pic = new PowerIterationClustering()
                .setK(2)
                .setMaxIterations(10);
        similarities.cache();

        PowerIterationClusteringModel model2 = pic.run(similarities);
        //now model2 contains two clusters
        JavaPairRDD<Integer,Long> clusters=model2.assignments().toJavaRDD().mapToPair(new PairFunction<PowerIterationClustering.Assignment, Integer, Long>() {
            @Override
            public Tuple2<Integer, Long> call(PowerIterationClustering.Assignment assignment) throws Exception {
                return new Tuple2<Integer,Long>(assignment.cluster(), assignment.id());
            }
        });

        //Printing Data into Text file
        JavaPairRDD<Integer,Iterable<Long>> groupedClusters=clusters.groupByKey();
        String book1="";

        for (Iterable<Long> longs : groupedClusters.lookup(0)) {
            for (Long aLong : longs) {
                book1=book1.concat(aLong.toString()+",");
            }
            book1=book1.substring(0,book1.length()-1);
        }

        String book2="";
        for (Iterable<Long> longs : groupedClusters.lookup(1)) {
            for (Long aLong : longs) {
                book2=book2.concat(aLong.toString()+",");
            }
            book2=book2.substring(0,book2.length()-1);
        }
        try {
            String Q1Output="Book1: "+book1+"\n Book2: "+book2;

            PrintWriter writer=new PrintWriter("Q1Output.txt","UTF-8");
            writer.print(Q1Output);
            writer.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        /**************************************************************
         Question 1 Complete
         **************************************************************/
//Creating an RDD with para num,shingle,paragraph index
        JavaPairRDD<Integer,Tuple2<String,Integer>> docShinglePara=input.flatMapToPair(new PairFlatMapFunction<String, Integer, Tuple2<String, Integer>>() {
            @Override
            public Iterator<Tuple2<Integer, Tuple2<String, Integer>>> call(String s) throws Exception {
                List<Tuple2<Integer,Tuple2<String,Integer>>> pairs=new ArrayList<>();
                if(!s.isEmpty() && s.charAt(0)!='\t') {
                    String str[] = s.split("\t");
                    Integer paraNo = Integer.parseInt(str[0]);
                    String para=str[1];
                    if(str[1].charAt(0)=='\"' || str[1].charAt(0)=='�')
                        para = str[1].substring(1, str[1].length() - 2);
                    int i=0;
                    while(i+5<para.length()) {
                        boolean flag=true;
                        for (Tuple2<Integer, Tuple2<String, Integer>> pair : pairs) {
                            if(para.substring(i, i + 5).equals(pair._2._1)){
                                flag=false;
                            }
                        }
                        if(flag)
                            pairs.add(new Tuple2<Integer,Tuple2<String,Integer>>(paraNo,new Tuple2<String,Integer>(para.substring(i, i + 5),i)));
                        i++;
                    }

                }
                return pairs.iterator();
            }
        });
        //This RDD is also for Question 2
        JavaPairRDD<Integer,Iterable<Tuple2<String,Integer>>> docShingle=docShinglePara.groupByKey();

        distanceMatrix.createOrReplaceTempView("distanceMatrix");
        //creating a temp view to run sql queries in it

        String query="SELECT * FROM distanceMatrix WHERE idA<>idB ORDER BY JaccardDistance ASC LIMIT 15";

        List<Row> book1List=spark.sql(query).collectAsList();

        //This method will remove pairs with (a,b) and (b,a)
        book1List=topFiveParas(book1List);

        Iterator iterator1=book1List.iterator();

        String output="";

        while (iterator1.hasNext()){
            Row row=(Row)iterator1.next();
            output=output.concat(getCommonShingles(docShingle,row.getInt(0),row.getInt(1)));
        }
        try {
            PrintWriter writer=new PrintWriter("Question2.txt","UTF-8");
            writer.print(output);
            writer.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static List<Row> topFiveParas(List<Row> rows){
        Iterator iterator=rows.iterator();
        int i=0;
        List<Row> notSame = new ArrayList<>();
        while (iterator.hasNext()){
            Row row=(Row)iterator.next();
            Iterator iterator2=rows.iterator();
            for (int k=0;k<=i;k++)
                iterator2.next();
            boolean flag=true;
            while (iterator2.hasNext()){
                Row row2=(Row)iterator2.next();
                if(row.getInt(0)==row2.getInt(1)&&row.getInt(1)==row2.getInt(0)){
                    flag=false;
                }
            }
            if(flag){
                 notSame.add(row);
            }
            i++;
        }
        Iterator iterator2=notSame.iterator();
        List<MySort> list=new ArrayList<>();
        while (iterator2.hasNext()){
            Row row=(Row)iterator2.next();
            list.add(new MySort(row.getInt(0),row.getInt(1),row.getDouble(2)));
        }
        Collections.sort(list);
        List<Row> top5=new ArrayList<>();
        Iterator iterator3=list.iterator();
        i=0;
        while(iterator3.hasNext()&&i<5){
            MySort elem=(MySort) iterator3.next();
            top5.add(RowFactory.create(elem.docID1,elem.docID2,elem.jaccardDistance));
            i++;
        }
        return top5;
    }
    public static String getCommonShingles(JavaPairRDD rdd, int docid1, int docid2){
        List<Tuple2<String,Integer>>list1=rdd.lookup(docid1);
        List<Tuple2<String,Integer>>list2=rdd.lookup(docid2);
        Iterator iterator1=list1.iterator();
        Iterator iterator2=list2.iterator();
        Iterable<Tuple2<String,Integer>> shingles1;
        Iterable<Tuple2<String,Integer>> shingles2;
        String output="\n";
        while (iterator1.hasNext()){
            //There is going to be only one item in the list which is the iterable of shingle,index
            shingles1=(Iterable<Tuple2<String,Integer>>)iterator1.next();

            Iterator iterator3= shingles1.iterator();

            while (iterator2.hasNext()){
                //There is going to be only one item in the list which is the iterable of shingle,index
                shingles2=(Iterable<Tuple2<String,Integer>>)iterator2.next();
                Iterator iterator4= shingles2.iterator();
                for (Tuple2<String, Integer> stringIntegerTuple2 : shingles1) {
                    for (Tuple2<String, Integer> integerTuple2 : shingles2) {
                        if(stringIntegerTuple2._1.equals(integerTuple2._1)){

                            String temp="<"+docid1+" - "+docid2+"><"+stringIntegerTuple2._1+"><"+stringIntegerTuple2._2+" - "+integerTuple2._2+">\n";
                            output=output.concat(temp);
                        }
                    }
                }
            }
        }
        output=output.concat("------------------------------------------------");
        return output;
    }
}
class MySort implements Comparable{
    public Integer docID1;
    public Integer docID2;
    public Double jaccardDistance;
    @Override
    public int compareTo(Object o) {
        if(((MySort)o).jaccardDistance-this.jaccardDistance>0)
            return -1;
        else if(((MySort)o).jaccardDistance-this.jaccardDistance<0)
            return 1;
        else
            return 0;
    }
    public MySort(int a,int b,double c){
        docID1=a;
        docID2=b;
        jaccardDistance=c;
    }
}
