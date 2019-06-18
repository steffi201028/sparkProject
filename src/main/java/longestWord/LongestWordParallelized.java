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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LongestWordParallelized implements LongestWord {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        new LongestWordParallelized().findLongestWords();

    }

    public static final String path = "/home/july/Projects/ProKo/sparkProject/languageFiles/";

    public JavaSparkContext sparkContext;

    public LongestWordParallelized (){
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("LongestWordsMax").set("spark.driver.allowMultipleContexts", "true");;
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        this.sparkContext = sparkContext;
    }

    public LongestWordParallelized (JavaSparkContext sparkContext){
        this.sparkContext = sparkContext;
    }

    public void findLongestWords() {

        String[] directories = new File(path).list();

        //getMaxWordsReduce(directories);
        Map <String, Tuple2<Integer, Iterable<String>>> maxWordsPerLanguage = getMaxWordsTupleComparator(directories);
        printLongestWordsWithLanguages(maxWordsPerLanguage.entrySet());

    }

    private Map <String, Tuple2<Integer, Iterable<String>>> getMaxWordsTupleComparator( String[] directories){

        Map<String, JavaRDD<String>> textPerLanguage = Arrays.asList(directories)
                .stream().collect(Collectors.toMap(
                        language -> language,
                        language -> sparkContext.textFile(path + language + "/*/*.txt")));

        Map<String, JavaRDD<String>> wordsPerLanguage = textPerLanguage
                .entrySet().stream().collect(Collectors.toMap(
                        textLanguageTuple -> textLanguageTuple.getKey(),
                        textLanguageTuple -> textLanguageTuple.getValue().flatMap(content -> Arrays.asList(content.split("(\\s|[...]|=|»|—|\\.|@|,|:|;|!|-|\\?|'|\\\")+")).iterator())));

        Map <String, JavaPairRDD<Integer, Iterable<String>>> countedWordsPerLanguage =   wordsPerLanguage
                .entrySet().stream().collect(Collectors.toMap(
                        wordsTuple -> wordsTuple.getKey(),
                        wordsTuple -> wordsTuple.getValue().distinct().mapToPair(t -> new Tuple2(t.length(),t))));


        Map <String, Tuple2<Integer, Iterable<String>>> withMaxKeys = countedWordsPerLanguage
                .entrySet().stream().collect(Collectors.toMap(
                        language -> language.getKey(),
                        language -> language.getValue().max(new TupleComparator())));

        return withMaxKeys;
    }

    private void printLongestWordsWithLanguages(Set<Map.Entry<String, Tuple2 <Integer,Iterable<String>>>> entrySet){
        entrySet.forEach(language -> {
            Tuple2 wordWithCount = language.getValue();
            System.out.println("Sprache:" + language.getKey() +  " Längstes Wort:" + wordWithCount._2 + " Länge: " + wordWithCount._1 );
        });
    }

    /*private void getMaxWordsReduce(String[] directories){

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("LongestWordsReduce");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        Map<String, JavaRDD<String>> textPerLanguage = Arrays.asList(directories)
                .stream().collect(Collectors.toMap(
                        language -> language,
                        language -> sparkContext.textFile(path + language + "/*//*.txt")));

        Map<String, JavaRDD<String>> wordsPerLanguage = textPerLanguage
                .entrySet().stream().collect(Collectors.toMap(
                        textLanguageTuple -> textLanguageTuple.getKey(),
                        textLanguageTuple -> textLanguageTuple.getValue().flatMap(content -> Arrays.asList(content.split(" ")).iterator())));

        Map <String, Tuple2<String, Integer>> countedWordsPerLanguage =   wordsPerLanguage
                .entrySet().stream().collect(Collectors.toMap(
                        wordsTuple -> wordsTuple.getKey(),
                        wordsTuple -> wordsTuple.getValue()
                                .distinct()
                                .mapToPair(t -> new Tuple2<>(t, t.length()))
                                .reduce((t1, t2) -> {
                                    return getLongerWord(t1,t2);
                                })));

         countedWordsPerLanguage.entrySet().forEach(language -> {
             Tuple2<String, Integer> wordWithCount = language.getValue();
             System.out.println("Sprache:" + language.getKey() +  " Längstes Wort:" + wordWithCount._1()+ " Länge: " + wordWithCount._2() );
         });

    }

    private String getLongerWord(String word1, String word2){
        if (word1.length() > word2.length()){
            return word1;
        }
        return word2;
    }

    private Tuple2 getLongerWord(Tuple2<String, Integer> word1, Tuple2<String,Integer> word2){
        if (word1._2() > word2._2()){
            return word1;
        }
        return word2;
    }*/
}
