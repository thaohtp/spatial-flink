package str_test;

import flink.RTree;
import flink.STRPacking;
import flink.datatype.Point;
import flink.datatype.RTreeNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 4/24/17.
 */
public class STRPackingTest {

    @Test
    public void testCreateTree2D() throws Exception {
        Log log = LogFactory.getLog(this.getClass());
        log.warn("Test create tree 2D");
        // Prepare data
        int nbDimension = 2;
        int nbPointPerNode = 3;
        List<Point> pointList = new ArrayList<Point>();
        pointList.add(TestUtil.create2DPoint(1, 1));
        pointList.add(TestUtil.create2DPoint(1, 2));
        pointList.add(TestUtil.create2DPoint(2, 2));
        pointList.add(TestUtil.create2DPoint(3, 9));
        pointList.add(TestUtil.create2DPoint(10, 4));
        pointList.add(TestUtil.create2DPoint(1, 5));
        pointList.add(TestUtil.create2DPoint(11, 10));

        STRPacking str = new STRPacking(nbPointPerNode, nbDimension);
        RTree rTree = str.createRTree(pointList);

        System.out.println("Test createCreateTree2D");
        System.out.println("-- Input points: ");
        for(int i =0; i<pointList.size(); i++){
            System.out.println(pointList.get(i));
        }

        System.out.println("-- Result tree: ");
        System.out.println(rTree.toString());
    }

    @Test
    public void testCreateTree3D() throws Exception {
        Log log = LogFactory.getLog(this.getClass());
        log.warn("Test create tree 3D");
        // Prepare data
        int nbDimension = 3;
        int nbPointPerNode = 3;
        List<Point> pointList = new ArrayList<Point>();
        pointList.add(TestUtil.create3DPoint(1, 1, 4));
        pointList.add(TestUtil.create3DPoint(1, 2, 5));
        pointList.add(TestUtil.create3DPoint(2, 2, 6));
        pointList.add(TestUtil.create3DPoint(3, 9, 7));
        pointList.add(TestUtil.create3DPoint(10, 4, 8));
        pointList.add(TestUtil.create3DPoint(1, 5, 9));
        pointList.add(TestUtil.create3DPoint(11, 10, 10));

        STRPacking str = new STRPacking(nbPointPerNode, nbDimension);
        RTree rTree = str.createRTree(pointList);

        System.out.println("Test createCreateTree2D");
        System.out.println("-- Input points: ");
        for(int i =0; i<pointList.size(); i++){
            System.out.println(pointList.get(i));
        }
        System.out.println("-- Result tree: ");
        System.out.println(rTree.toString());
    }

    @Test
    public void testSearch2D() throws Exception {
        // Prepare data
        Log log = LogFactory.getLog(this.getClass());
        log.warn("Test create tree 2D");
        // Prepare data
        int nbDimension = 2;
        int nbPointPerNode = 3;
        List<Point> pointList = new ArrayList<Point>();
        pointList.add(TestUtil.create2DPoint(1, 1));
        pointList.add(TestUtil.create2DPoint(1, 2));
        pointList.add(TestUtil.create2DPoint(2, 2));
        pointList.add(TestUtil.create2DPoint(3, 9));
        pointList.add(TestUtil.create2DPoint(10, 4));
        pointList.add(TestUtil.create2DPoint(1, 5));
        pointList.add(TestUtil.create2DPoint(11, 10));

        STRPacking str = new STRPacking(nbPointPerNode, nbDimension);
        RTree rTree = str.createRTree(pointList);

        System.out.println("Test createCreateTree2D");
        System.out.println("--Input points: ");
        for(int i =0; i<pointList.size(); i++){
            System.out.println(pointList.get(i));
        }

        System.out.println("--Result tree: ");
        System.out.println(rTree.toString());

        System.out.println("--Search (3,9): ");
        List<RTreeNode> searchNodes = rTree.search(TestUtil.create2DPoint(3,9));
        for(int i =0; i<searchNodes.size(); i++){
            System.out.println(searchNodes.get(i));
        }

        System.out.println("--Search (10,10): ");
        searchNodes = rTree.search(TestUtil.create2DPoint(10,10));
        for(int i =0; i<searchNodes.size(); i++){
            System.out.println(searchNodes.get(i));
        }

        System.out.println("--Search (10,5): ");
        searchNodes = rTree.search(TestUtil.create2DPoint(10,5));
        for(int i =0; i<searchNodes.size(); i++){
            System.out.println(searchNodes.get(i));
        }

        System.out.println("--Search (10,15): ");
        searchNodes = rTree.search(TestUtil.create2DPoint(10,15));
        for(int i =0; i<searchNodes.size(); i++){
            System.out.println(searchNodes.get(i));
        }
    }

}
