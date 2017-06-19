package de.tu_berlin.dima.util;

import de.tu_berlin.dima.datatype.Point;

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
        if(p1.getDimension(sortDimension) > p2.getDimension(sortDimension)){
            return 1;
        }
        else{
            if(p1.getDimension(sortDimension) < p2.getDimension(sortDimension)){
                return -1;
            }
            else return 0;
        }

    }
}
