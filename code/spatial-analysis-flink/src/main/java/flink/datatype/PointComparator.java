package flink.datatype;

import java.util.Comparator;

/**
 * Created by JML on 3/7/17.
 */
public class PointComparator implements Comparator<Point>{
    int sortDimension;

    public PointComparator(int dimension){
        this.sortDimension = dimension;
    }

    @Override
    public int compare(Point p1, Point p2) {
        if(p1.getValueOfDimension(sortDimension) > p2.getValueOfDimension(sortDimension)){
            return 1;
        }
        else{
            if(p1.getValueOfDimension(sortDimension) < p2.getValueOfDimension(sortDimension)){
                return -1;
            }
            else return 0;
        }

    }
}
