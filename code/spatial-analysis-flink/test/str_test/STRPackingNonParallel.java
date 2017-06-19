package str_test;

import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.STRPacking;
import de.tu_berlin.dima.datatype.Point;
import de.tu_berlin.dima.datatype.RTreeNode;
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
        pointList.add(create2DPoint(1, 1));
        pointList.add(create2DPoint(1, 2));
        pointList.add(create2DPoint(2, 2));
        pointList.add(create2DPoint(3, 9));
        pointList.add(create2DPoint(10, 4));
        pointList.add(create2DPoint(1, 5));
        pointList.add(create2DPoint(11, 10));

        // < (1.0,1.0)(1.0,2.0)(2.0,2.0) , (1.0,5.0)(3.0,9.0) , (10.0,4.0)(11.0,10.0) ,  >

//        pointList.add(create2DPoint(3, 9));
//        pointList.add(create2DPoint(10, 4));
//        pointList.add(create2DPoint(1, 5));
//        pointList.add(create2DPoint(11, 10));

//        pointList.add(create3DPoint(1, 1, 4));
//        pointList.add(create3DPoint(1, 2, 5));
//        pointList.add(create3DPoint(2, 2, 6));
//        pointList.add(create3DPoint(3, 9, 7));
//        pointList.add(create3DPoint(10, 4, 8));
//        pointList.add(create3DPoint(1, 5, 9));
//        pointList.add(create3DPoint(11, 10, 10));

        STRPacking str = new STRPacking(3, 2);
        RTree rTree = str.createRTree(pointList);

        System.out.println(rTree.toString());

        // Test case: for searching (exactly the point + in the MBR but not exist + not exist in the tree)
        // Test case: (11,10) (10,10) (10,15)
        List<RTreeNode> searchNodes = rTree.search(pointList.get(6));
        System.out.println("Testing searching result");
        for(int i =0; i<searchNodes.size(); i++){
            System.out.println(searchNodes.get(i));
        }

    }

    private static Point create2DPoint(float x, float y){
        List<Float> floatList = new ArrayList<Float>(2);
        floatList.add(0, x);
        floatList.add(1, y);
        return new Point(floatList);
    }

    private static Point create3DPoint(float x, float y, float z){
        List<Float> floatList = new ArrayList<Float>(3);
        floatList.add(0, x);
        floatList.add(1, y);
        floatList.add(2, z);
        return new Point(floatList);
    }
}
