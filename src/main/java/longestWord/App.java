package longestWord;


import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.io.*;
import java.util.Arrays;
import java.util.Scanner;
import org.apache.spark.api.java.JavaSparkContext;


public class App {
    public static void main(String[] args) throws FileNotFoundException {
        new App().gettest();

    }

  /*  public void findLongestWords() throws FileNotFoundException {

        String idStr = "";
        String longest_word = "";
        String path = "";

        File[] directories = new File("C:/Users/Stephanie/Documents/AI_Master/ProgrammAlgo/longestWord/languageFiles").listFiles(File::isDirectory);


        for (int i = 0; i < directories.length; i++) {
            idStr = new File(directories[i].getPath()).getName();
            //System.out.println("Sprache: " + idStr);
            String textFilespath = "C:/Users/Stephanie/Documents/AI_Master/ProgrammAlgo/longestWord/languageFiles/" + idStr + "/TXT";
            // System.out.println(textFilespath);
            File[] filesPerLanguage = new File(textFilespath).listFiles(File::isFile);
            longest_word = "";
            for (int j = 0; j < filesPerLanguage.length; j++) {
                String sb = "";
                File filename = (new File(filesPerLanguage[j].getPath()));
                try {
                    sb = getFileContent(filename);
                } catch (IOException e) {
                    e.printStackTrace();
                }


                String current;
                //path ="C:/Users/Stephanie/Documents/AI_Master/ProgrammAlgo/longestWord/languageFiles/"+idStr+"/TXT/"+filename;
                String longestWordOfFile = "";
                // System.out.println(path);
                Scanner sc = new Scanner(sb);
                // sc.useDelimiter("-|«| |\n");
                sc.useDelimiter("(\\s|=|—|\\.|@|,|:|;|!|-|\\?|'|\\\")+");
                //sc.useDelimiter("[-«,!?.]+");

                while (sc.hasNext()) {
                    current = sc.next();
                    if (!current.contains("……") && (!current.contains("[LocalizedFileNames]"))) {
                        if (current.length() > longestWordOfFile.length() && current.length() > longest_word.length()) {
                            longestWordOfFile = current;
                            longest_word = longestWordOfFile;
                        }
                    }

                }

            }

            System.out.print(idStr);
            System.out.print("-- " + longest_word);
            System.out.print("-- " + longest_word.length());

            //return longest_word;

        }

    }

    public static String getFileContent(File file) throws IOException {
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "UTF8"))) {

            String sCurrentLine;

            while ((sCurrentLine = br.readLine()) != null) {
                //System.out.println(sCurrentLine);
                sb.append(sCurrentLine);
                sb.append(System.getProperty("line.separator"));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }*/

    public void gettest(){
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("spark://192.168.56.1:7077");
         JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile("C:\\Users\\Stephanie\\spark\\spark-2.4.2-bin-hadoop2.7\\README.md");
        JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b );
        //System.out.println("Hallo");
        counts.saveAsTextFile("C:\\Users\\Stephanie\\spark\\spark-2.4.2-bin-hadoop2.7\\results.txt");

    }
}
