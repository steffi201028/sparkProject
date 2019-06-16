package longestWord;

import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;

public class TupleComparator implements Comparator<Tuple2<Integer, Iterable<String>>>, Serializable {
    @Override
    public int compare(Tuple2<Integer, Iterable<String>> x, Tuple2<Integer, Iterable<String>> y) {
        return Integer.compare(x._1(), y._1());
    }
}