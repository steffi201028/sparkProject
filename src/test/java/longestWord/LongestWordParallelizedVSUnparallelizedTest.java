package longestWord;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Unit test for simple LongestWordUnparallelized.
 */
public class LongestWordParallelizedVSUnparallelizedTest
{

    public static LongestWordParallelizedReduce longestWordParallelizedFinder;
    public static LongestWordUnparallelized longestWordUnparallelizedFinder;
    public static String path;
    public static String[] directoriesMultipleLanguagesMultipleText;
    public static String LONGEST_WORD;

    @BeforeAll
    public static void onceExecutedBeforeAll() throws IOException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.out.println("Setting it up test suite...");

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("LongestWordsTimeTest");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        longestWordParallelizedFinder = new LongestWordParallelizedReduce(sparkContext);
        longestWordUnparallelizedFinder = new LongestWordUnparallelized();

        LONGEST_WORD = "loooooooooooooooooongestWord";

        path = "/home/july/Projects/ProKo/github/sparkProject/testLanguageFiles/";

        String directoriesOneLanguageOneText = "languageOneText";
        String directoriesOneLanguageMultipleText = "languageMultipleTexts";
        directoriesMultipleLanguagesMultipleText = new String[]{directoriesOneLanguageOneText, directoriesOneLanguageMultipleText};

        new File(path + directoriesOneLanguageOneText + "/TXT/").mkdirs();
        new File(path + directoriesOneLanguageMultipleText + "/TXT/").mkdirs();

        FileWriter writer1 = new FileWriter(path + directoriesOneLanguageOneText + "/TXT/text1.txt");
        writer1.write("word&% ? #' \"word\" ... word word wooooord " + LONGEST_WORD + " other words");

        FileWriter writer2 = new FileWriter(path + directoriesOneLanguageMultipleText + "/TXT/text2.txt");
        writer2.write("word word word word wooooord " + LONGEST_WORD + " other words");

        FileWriter writer3 = new FileWriter(path + directoriesOneLanguageMultipleText + "/TXT/text3.txt");
        writer3.write("word word word word wooooord " + LONGEST_WORD + " other words " + "...." + LONGEST_WORD);

        writer1.close();
        writer2.close();
        writer3.close();
    }

    @AfterAll
    public static void onceExecutedAfterAll() throws IOException {

        FileUtils.deleteDirectory(new File(path));
    }

    @Test
    public void testParallelizedGetSameLongestWordAsUnparallelized()
    {
        File[] directories = new File(path).listFiles(File::isDirectory);
        JavaPairRDD<Integer, Tuple2<String, String>> longestWordsSortedParallelized = longestWordParallelizedFinder.getLongestWordsSorted(path, directoriesMultipleLanguagesMultipleText);
        NavigableMap<Integer, Tuple2<String, String>> longestWordsSortedUnparallelized = longestWordUnparallelizedFinder.getLongestWordsSorted(path, directories);

        assertEquals(longestWordsSortedParallelized.first()._2()._2(),
                longestWordsSortedUnparallelized.firstEntry().getValue()._2());
    }


    @Test
    public void testParallelizedShorterExecutionTimeThanUnparallelized()
    {
        assertEquals(true, getDuration(longestWordParallelizedFinder) < getDuration(longestWordUnparallelizedFinder));
    }

    private long getDuration(LongestWord finder){
        long startTime = System.nanoTime();

        finder.findLongestWords();

        long endTime = System.nanoTime();

        return (endTime - startTime);
    }
}
