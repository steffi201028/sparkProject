package longestWord;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;


import static java.util.Arrays.asList;

public class test {

        public static void main(String[] args) {

            //SparkSession spark = SparkSession.builder().master("spark://192.168.56.1:7077").appName("Test1").getOrCreate();
            SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("TestNew");
            JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


            JavaRDD <String> textPerLanguage = sparkContext.textFile("C:\\Users\\Stephanie\\Documents\\AI_Master\\ProgrammAlgo\\longestWord\\languageFiles\\English\\*\\*.txt");
JavaRDD<String> newRdd = textPerLanguage.flatMap(content -> asList(content.split("(\\s|=|»|—|\\.|@|,|:|;|!|-|\\?|'|\\\")+")).iterator());

         JavaPairRDD <Integer, Iterable<String>> counts =   newRdd.distinct().mapToPair(t -> new Tuple2(t.length(),t));
        // counts = counts.sortByKey();








            for( Object line:counts.collect()){
                System.out.println("* "+line);
            }

            Tuple2<Integer, Iterable<String>> withMaxKeys = counts.max(new TupleComparator());

           // String maxKeys = counts.keys().max (Comparator.comparingInt(y -> Integer.parseInt(y)));

            // JavaPairRDD<String, Integer> withMaxKeys = counts.filter (y -> y._2.equals(maxKeys));

            System.out.println("Längstes Wort:" + withMaxKeys._2 + " Länge: " + withMaxKeys._1 );



            //System.out.println("JJAJAJAJAJA");
//counts.saveAsTextFile("out2");
           //logData.flatMap(content -> Arrays.asList(content.split(" ")).iterator()).saveAsTextFile("out");
            //JavaRDD<String> inputFile = spark.textFile("C:\\\\Users\\\\Stephanie\\\\Documents\\\\AI_Master\\\\ProgrammAlgo\\\\longestWord\\\\languageFiles\\\\Deutsch\\\\TXT\\\\Bis zum Nullpunkt des Seins - Kurd Lasswitz.txt");
            //JavaRDD<String> words = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
            //String longest = words.max(new LengthComparator());
           // words.saveAsTextFile("out");

            sparkContext.stop();

        }


    public static class TupleComparator implements Comparator<Tuple2<Integer, Iterable<String>>>, Serializable {
        @Override
        public int compare(Tuple2<Integer, Iterable<String>> x, Tuple2<Integer, Iterable<String>> y) {
            return Integer.compare(x._1(), y._1());
        }
    }



}




