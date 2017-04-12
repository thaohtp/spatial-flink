package flink;

import flink.datatype.Point;
import flink.datatype.RTreeNode;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 4/5/17.
 */
public class STRPackingNonParallel {
    public static void main(String[] args) throws Exception {
        List<Point> pointList = new ArrayList<Point>();
        pointList.add(new Point(1, 1));
        pointList.add(new Point(1, 2));
        pointList.add(new Point(2, 2));
        pointList.add(new Point(3, 9));
        pointList.add(new Point(10, 4));
        pointList.add(new Point(1, 5));
        pointList.add(new Point(11, 10));


//        pointList.add(new Point(3, 9));
//        pointList.add(new Point(10, 4));
//        pointList.add(new Point(1, 5));
//        pointList.add(new Point(11, 10));

//        pointList.add(new Point(1, 1, 4));
//        pointList.add(new Point(1, 2, 5));
//        pointList.add(new Point(2, 2, 6));
//        pointList.add(new Point(3, 9, 7));
//        pointList.add(new Point(10, 4, 8));
//        pointList.add(new Point(1, 5, 9));
//        pointList.add(new Point(11, 10, 10));


        STRPacking str = new STRPacking(3, 2, pointList);
        RTree rTree = str.createRTree();

        System.out.println(rTree.toString());

    }
}
