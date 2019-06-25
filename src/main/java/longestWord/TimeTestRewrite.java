package longestWord;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


public class TimeTestRewrite {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);


        SparkConf sparkConf = new SparkConf().setAppName("LongestWordsTimeTest").set("spark.driver.allowMultipleContexts", "true");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


        LongestWord[] longestWords = new LongestWord[]{

                 // new LongestWordParallelized(sparkContext),
                  new LongestWordParallelizedReduce(sparkContext),
                  new LongestWordParallelizedTupleComparator(sparkContext),
                  new LongestWordUnparallelized()};

        long DurationOverall = 0;
        for(int j = 1; j <= 10; ++j) {

            System.out.println("Test running...");

            long[] times = new long[longestWords.length];
            for (int i = 0; i < longestWords.length; i++) {
                long startTime = System.nanoTime();

                longestWords[i].findLongestWords();

                long endTime = System.nanoTime();

                long duration = (endTime - startTime);

                times[i] = duration;
               // System.out.println("Dauer von Durchlauf " + j + ":" + duration/1000000000);
                DurationOverall += times[i];
            }

           System.out.println("Durchlauf" + j + ":");

            for (int i = 0; i < times.length; i++) {

                System.out.println("-------------------------------------------------------------------------------");
                //System.out.println(longestWords[i].getClass() + ": " + 1. * times[i] / 1000000000 + " seconds");
                System.out.println("Dauer von Durchlauf " + j + ": " + 1. * times[i] / 1000000000 + " seconds");
                System.out.println("-------------------------------------------------------------------------------");
            }
            System.out.println("Overall Duration nach Durchlauf " + j +" ist " + 1. * DurationOverall/1000000000);
            System.out.println();


        }
        long number = 10;
        System.out.println("-------------------------------------------------------------------------------");
        System.out.println("Average Duration von LongestWordUnparallelized ist: " + 1. * DurationOverall/1000000000/(number)  + " seconds");
        System.out.println("-------------------------------------------------------------------------------");

    }
}
