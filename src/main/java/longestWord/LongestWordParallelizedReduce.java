package longestWord;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.util.Arrays;

public class LongestWordParallelizedReduce implements LongestWord {

    public static final String path = "/home/july/Projects/ProKo/sparkProject/languageFiles/";

    public JavaSparkContext sparkContext;

    public LongestWordParallelizedReduce(){
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("LongestWordsSingle").set("spark.driver.allowMultipleContexts", "true");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        this.sparkContext = sparkContext;
    }

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        new LongestWordParallelizedReduce().findLongestWords();

    }

    public void findLongestWords(){
        String[] directories = new File(path).list();

        for (int i = 0; i<directories.length; i++){
            findLongestWord(directories[i]);
        }
        sparkContext.stop();
    }

    public void findLongestWord(String language) {

        JavaRDD<String> languageText = sparkContext.textFile(path + language + "/*/*.txt");

        JavaRDD<String> words= languageText.flatMap(content -> Arrays.asList(content.split("(\\s|[...]|=|»|—|\\.|@|,|:|;|!|-|\\?|'|\\\")+")).iterator());

        JavaPairRDD<Integer, Iterable<String>> countedWords =   words.distinct().mapToPair(t -> new Tuple2(t.length(),t));

        Tuple2<Integer, Iterable<String>> longestWord = countedWords.reduce((t1, t2) -> { if(t1._1 > t2._1){return t1;}else{ return t2;}});

        System.out.println("Sprache:" + language +  " Längstes Wort:" + longestWord._1()+ " Länge: " + longestWord._2() );

    }
}
