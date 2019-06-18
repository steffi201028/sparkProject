package longestWord;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class TimeTest {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);


        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("LongestWordsTimeTest").set("spark.driver.allowMultipleContexts", "true");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        LongestWord[] longestWords = new LongestWord[]{
                new LongestWordParallelized(sparkContext),
                new LongestWordParallelizedReduce(sparkContext),
                new LongestWordUnparallelized()};

        long[] times = new long[longestWords.length];
        for(int i = 0; i < longestWords.length; i++){
            long startTime = System.nanoTime();

            longestWords[i].findLongestWords();

            long endTime = System.nanoTime();

            long duration = (endTime - startTime);

            times[i] = duration;
            System.out.println("-------------------------------------------------------------------------------");
            System.out.println(longestWords[i].getClass() + ": " + duration);
            System.out.println("-------------------------------------------------------------------------------");
        }
        for(int i = 0; i < times.length; i++){
            System.out.println("-------------------------------------------------------------------------------");
            System.out.println(longestWords[i].getClass() + ": " + 1. * times[i] / 1000000000 + " seconds");
            System.out.println("-------------------------------------------------------------------------------");
        }
    }
}
