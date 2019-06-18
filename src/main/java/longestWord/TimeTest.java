package longestWord;

public class TimeTest {

    public static void main(String[] args) {

        LongestWord[] longestWords = new LongestWord[]{
                new LongestWordParallelized(),
                new LongestWordParallelizedReduce(),
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
            System.out.println(longestWords[i].getClass() + ": " + times[i] / 1000000 + " seconds");
            System.out.println("-------------------------------------------------------------------------------");
        }
    }
}
