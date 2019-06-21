package longestWord;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

/**
 * Unit test for simple LongestWordUnparallelized.
 */
public class LongestWordParallelizedReduceTest
{

    public static LongestWordParallelizedReduce longestWordFinder;
    public static String path;
    public static String[] directoriesMultipleLanguagesMultipleText;
    public static String directoriesOneLanguageOneText;
    public static String directoriesOneLanguageMultipleText;
    public static String LONGEST_WORD;

    @BeforeAll
    public static void onceExecutedBeforeAll() throws IOException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        System.out.println("Setting it up test suite...");

        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("LongestWordsTimeTest");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        longestWordFinder = new LongestWordParallelizedReduce(sparkContext);

        LONGEST_WORD = "loooooooooooooooooongestWord";

        path = "/home/july/Projects/ProKo/github/sparkProject/testLanguageFiles/";

        directoriesOneLanguageOneText = "languageOneText";
        directoriesOneLanguageMultipleText = "languageMultipleTexts";
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
    public void testGetLongestWordsSorted_noLanguages()
    {
        JavaPairRDD longestWordsSorted = longestWordFinder.getLongestWordsSorted(path, new String[0]);
        assertEquals(true, longestWordsSorted.isEmpty(),  "Collection of found longest words should be empty.");
    }

    @Test
    public void testGetLongestWordsSorted_oneLanguageOneText()
    {
        JavaPairRDD longestWordsSorted = longestWordFinder.getLongestWordsSorted(path, new String[]{directoriesOneLanguageOneText});
        assertEquals(1, longestWordsSorted.collect().size());
    }

    @Test
    public void testGetLongestWordsSorted_multipleLanguageTexts()
    {
        JavaPairRDD longestWordsSorted = longestWordFinder.getLongestWordsSorted(path, directoriesMultipleLanguagesMultipleText);
        assertEquals(true, longestWordsSorted.collect().size() > 1);
    }

    @Test
    public void testGetLongestWordsSorted_noSpecialCharacters()
    {
        JavaPairRDD<Integer, Tuple2<String, String>> longestWordsSorted = longestWordFinder.getLongestWordsSorted(path, directoriesMultipleLanguagesMultipleText);

        Pattern p = Pattern.compile("[^a-z ]", Pattern.CASE_INSENSITIVE);
        for(Tuple2<Integer, Tuple2<String, String>> tuple : longestWordsSorted.collect()) {
            assertFalse( p.matcher(tuple._2()._2()).find());
        }
    }

    @Test
    public void testGetLongestWordsSorted_wordSizeCorrect()
    {
        JavaPairRDD<Integer, Tuple2<String, String>> longestWordsSorted = longestWordFinder.getLongestWordsSorted(path, directoriesMultipleLanguagesMultipleText);

        for(Tuple2<Integer, Tuple2<String, String>> tuple : longestWordsSorted.collect()) {
            assertEquals(tuple._1(), tuple._2()._2().length());
        }
    }

    @Test
    public void testGetLongestWordsSorted_correctlySorted()
    {
        JavaPairRDD<Integer, Tuple2<String, String>> longestWordsSorted = longestWordFinder.getLongestWordsSorted(path, directoriesMultipleLanguagesMultipleText);

        int previousSize = Integer.MAX_VALUE;
        for(Tuple2<Integer, Tuple2<String, String>> tuple : longestWordsSorted.collect()) {
            assertEquals(true, tuple._1() <= previousSize);
            previousSize = tuple._1();
        }
    }

    @Test
    public void testGetLongestWordsSorted_foundLongestWord()
    {
        JavaPairRDD<Integer, Tuple2<String, String>> longestWordsSorted = longestWordFinder.getLongestWordsSorted(path, new String[]{directoriesOneLanguageOneText});
        assertEquals(LONGEST_WORD, longestWordsSorted.first()._2()._2());
    }
}
