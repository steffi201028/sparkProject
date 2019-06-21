package longestWord;

import scala.Tuple2;

import java.io.*;
import java.util.*;


public class LongestWordUnparallelized implements LongestWord{

    public static final String path = "/home/july/Projects/ProKo/sparkProject/languageFiles/";

    public static void main(String[] args) {
        new LongestWordUnparallelized().findLongestWords();

    }

    public void findLongestWords() {
        File[] directories = new File(path).listFiles(File::isDirectory);

        Map<Integer, Tuple2<String, String>> longestWords = getLongestWordsSorted(path, directories);

        printLongestWordsWithLanguages(longestWords);
    }

    public NavigableMap<Integer, Tuple2<String, String>> getLongestWordsSorted(String pathToLanguageFiles, File[] directories){

        String idStr = "";
        String longest_word = "";
        TreeMap<Integer, Tuple2<String, String>> longestWordsPerLanguage = new TreeMap<>();

        for (int i = 0; i < directories.length; i++) {
            idStr = new File(directories[i].getPath()).getName();
            //System.out.println("Sprache: " + idStr);
            String textFilespath = pathToLanguageFiles + idStr + "/TXT";
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
                sc.useDelimiter("(\\s|[^\\p{L}]|=|—|\\.|@|,|:|;|!|-|\\?|'|\\\")+");
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

            longestWordsPerLanguage.put(longest_word.length(), new Tuple2<>(idStr, longest_word));
        }

        return longestWordsPerLanguage.descendingMap();

    }

    private void printLongestWordsWithLanguages(Map<Integer, Tuple2<String, String>>  longestWords){
        for(Map.Entry<Integer, Tuple2<String, String>> entry : longestWords.entrySet()) {
            System.out.println(entry.getValue()._1 + " - " + entry.getValue()._2 + " - " + entry.getKey());
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
    }
}
