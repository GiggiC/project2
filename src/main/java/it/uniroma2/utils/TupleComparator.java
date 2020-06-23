package it.uniroma2.utils;

import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;

public class TupleComparator implements Comparator<Tuple2<String, Integer>>, Serializable {

    @Override
    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {

        int count_1 = o1._2;
        int count_2 = o2._2;

        if (count_1 < count_2) {

            return 1;

        } else if (count_1 == count_2) {

            return 0;
        }

        return -1;
    }
}
