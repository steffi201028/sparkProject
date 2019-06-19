package longestWord;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.*;

public class LongestWordParallelizedTupleComparator implements LongestWord {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        new LongestWordParallelizedTupleComparator().findLongestWords();

    }

    public static final String path = "/home/july/Projects/ProKo/sparkProject/languageFiles/";

    public JavaSparkContext sparkContext;

    public LongestWordParallelizedTupleComparator(){
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("LongestWordsMax").set("spark.driver.allowMultipleContexts", "true");;
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        this.sparkContext = sparkContext;
    }

    public LongestWordParallelizedTupleComparator(JavaSparkContext sparkContext){
        this.sparkContext = sparkContext;
    }

    public void findLongestWords() {

        String[] directories = new File(path).list();

        List<Tuple2<Integer, Tuple2<String, String>>> maxWordsPerLanguage = new ArrayList<>();

        for (int i = 0; i<directories.length; i++){
            String language = directories[i];
            JavaRDD<String> wordsFromText = sparkContext
                    .textFile(path + language + "/*/*.txt")
                    .flatMap(content -> Arrays.asList(content.split("(\\s|[^\\p{L}]|=|»|—|\\.|@|,|:|;|!|-|\\?|'|\\\")+")).iterator());


            JavaPairRDD<Integer, Iterable<String>> sizedWords = wordsFromText
                    .distinct()
                    .mapToPair(t -> new Tuple2(t.length(),t));

            Tuple2<Integer, Iterable<String>> maxWord = sizedWords.max(new TupleComparator());
            maxWordsPerLanguage.add(new Tuple2(maxWord._1(), new Tuple2<>(language, String.valueOf(maxWord._2()))));

        }

        JavaPairRDD maxWordsSorted = sparkContext.parallelizePairs(maxWordsPerLanguage).sortByKey(false);

        printLongestWordsWithLanguages(maxWordsSorted);
    }

    private void printLongestWordsWithLanguages(JavaPairRDD<Integer, Tuple2<String, String>>  longestWords){
        for(Tuple2<Integer, Tuple2<String, String>> tuple : longestWords.collect()) {
            System.out.println(tuple._2._1 + " - " + tuple._2._2 + " - " + tuple._1);
        }
    }
}
