package str_test;

import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.STRPacking;
import de.tu_berlin.dima.datatype.Point;
import de.tu_berlin.dima.datatype.RTreeNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 4/24/17.
 */
public class STRPacking3DTest {
    private int POINTS_PER_NODE = 3;
    private int NB_DIMENSION = 3;
    private List<Point> pointList = new ArrayList<Point>();


    private void prepareData(){
        this.pointList.clear();
        pointList.add(TestUtil.create3DPoint(1, 1, 4));
        pointList.add(TestUtil.create3DPoint(1, 2, 5));
        pointList.add(TestUtil.create3DPoint(2, 2, 6));
        pointList.add(TestUtil.create3DPoint(3, 9, 7));
        pointList.add(TestUtil.create3DPoint(10, 4, 8));
        pointList.add(TestUtil.create3DPoint(1, 5, 9));
        pointList.add(TestUtil.create3DPoint(11, 10, 10));
    }

    @Test
    public void testCreateTree3D() throws Exception {
        Log log = LogFactory.getLog(this.getClass());
        log.warn("Test create tree 3D");
        prepareData();

        STRPacking str = new STRPacking(this.POINTS_PER_NODE, this.NB_DIMENSION);
        RTree rTree = str.createRTree(this.pointList);

        System.out.println("Test createCreateTree2D");
        System.out.println("-- Input points: ");
        for(int i =0; i<this.pointList.size(); i++){
            System.out.println(this.pointList.get(i));
        }
        System.out.println("-- Result tree: ");
        System.out.println(rTree.toString());
    }


}
