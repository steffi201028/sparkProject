package longestWord;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;

public class LongestWordParallelized {

    public static void main(String[] args) {
        new LongestWordParallelized().findLongestWords();

    }

    public void findLongestWords() {

        String path = "/home/july/Projects/ProKo/sparkProject/languageFiles/";
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("TestNew");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


        String[] directories = new File(path).list();
        Map<String, JavaRDD<String>> textPerLanguage = Arrays.asList(directories).stream().collect(
                Collectors.toMap(language -> language, language -> sparkContext.textFile(path + language + "/*/*.txt")));

        Map<String, JavaRDD<String>> wordsPerLanguage = textPerLanguage.entrySet().stream().collect(Collectors.toMap(
                textLanguageTuple -> textLanguageTuple.getKey(),
                textLanguageTuple -> textLanguageTuple.getValue().flatMap(content -> Arrays.asList(content.split("(\\s|=|»|—|\\.|@|,|:|;|!|-|\\?|'|\\\")+")).iterator())));

        Map <String, JavaPairRDD<Integer, Iterable<String>>> countedWordsPerLanguage =   wordsPerLanguage.entrySet().stream().collect(Collectors.toMap(
                wordsTuple -> wordsTuple.getKey(),
                wordsTuple -> wordsTuple.getValue().distinct().mapToPair(t -> new Tuple2(t.length(),t))));


        //JavaRDD<String> textPerLanguage = sparkContext.textFile("/home/july/Projects/ProKo/sparkProject/languageFiles/English/*/*.txt");
        //JavaRDD<String> newRdd = textPerLanguage.flatMap(content -> asList(content.split("(\\s|=|»|—|\\.|@|,|:|;|!|-|\\?|'|\\\")+")).iterator());

        //JavaPairRDD<Integer, Iterable<String>> counts =   newRdd.distinct().mapToPair(t -> new Tuple2(t.length(),t));

        /*for( Object line:counts.collect()){
            System.out.println("* "+line);
        }*/

        Map <String, Tuple2<Integer, Iterable<String>>> withMaxKeys = countedWordsPerLanguage.entrySet().stream().collect(Collectors.toMap(
                language -> language.getKey(),
                language -> language.getValue().max(new TupleComparator())));

        // String maxKeys = counts.keys().max (Comparator.comparingInt(y -> Integer.parseInt(y)));

        // JavaPairRDD<String, Integer> withMaxKeys = counts.filter (y -> y._2.equals(maxKeys));

        withMaxKeys.entrySet().forEach(language -> {
            Tuple2 wordWithCount = language.getValue();
            System.out.println("Sprache:" + language.getKey() +  " Längstes Wort:" + wordWithCount._2 + " Länge: " + wordWithCount._1 );
        });





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
