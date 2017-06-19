package str_test;

import de.tu_berlin.dima.IndexBuilder;
import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.STRPacking;
import de.tu_berlin.dima.STRPartitioner;
import de.tu_berlin.dima.datatype.MBR;
import de.tu_berlin.dima.datatype.PointLeafNode;
import de.tu_berlin.dima.datatype.Point;
import de.tu_berlin.dima.datatype.RTreeNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 4/24/17.
 */
public class STRPartitionerParallel2DTest {
    private static final int POINTS_PER_NODE = 3;
    private static final int NB_DIMENSION = 2;
    private List<Point> pointList = new ArrayList<Point>();


    private void prepareData(){
        this.pointList.clear();
        pointList.add(TestUtil.create2DPoint(1, 0));
        pointList.add(TestUtil.create2DPoint(1, 2));
        pointList.add(TestUtil.create2DPoint(2, 2));
        pointList.add(TestUtil.create2DPoint(3, 9));
        pointList.add(TestUtil.create2DPoint(10, 4));
        pointList.add(TestUtil.create2DPoint(-1, 5));
        pointList.add(TestUtil.create2DPoint(11, 10));
    }

    private void prepareData6Points(){
        this.pointList.clear();
        pointList.add(TestUtil.create2DPoint(1, 0));
        pointList.add(TestUtil.create2DPoint(1, 2));
        pointList.add(TestUtil.create2DPoint(2, 2));
        pointList.add(TestUtil.create2DPoint(3, 9));
        pointList.add(TestUtil.create2DPoint(10, 4));
        pointList.add(TestUtil.create2DPoint(-1, 5));
    }

    private void prepareData7Points(){
        prepareData6Points();
        pointList.add(TestUtil.create2DPoint(11,10));
    }

    private void prepareData8Points(){
        prepareData7Points();
        pointList.add(TestUtil.create2DPoint(12,-10));
    }

    private void prepareData9Points(){
        prepareData8Points();
        pointList.add(TestUtil.create2DPoint(11,11));
    }

    private void prepareData10Points(){
        prepareData9Points();
        pointList.add(TestUtil.create2DPoint(15,8));
    }

    private RTree createAndPrintTree(List<Point> points, int pointPerNode, int nbDimension) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Point> dataSet = env.fromCollection(this.pointList);

        double sampleRate = 0.5;
        int parallelism = env.getParallelism();

        IndexBuilder indexBuilder = new IndexBuilder();
        STRPartitioner partitioner = indexBuilder.createSTRPartitionerTestVersion(dataSet, dataSet, nbDimension, pointPerNode, sampleRate, parallelism);
        RTree rTree = partitioner.getrTree();

        System.out.println("Test createCreateTree2D");
        System.out.println("-- Input points: ");
        for(int i =0; i<points.size(); i++){
            System.out.println(points.get(i));
        }

        System.out.println("-- Result tree: ");
        System.out.println(rTree.toString());
        return rTree;
    }

    @Test
    public void testCreateTree2D() throws Exception {
        Log log = LogFactory.getLog(this.getClass());
        log.warn("Test create tree 2D");
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

    @Test
    public void testCreate6Points() throws Exception{
        prepareData6Points();
        RTree rTree = createAndPrintTree(this.pointList, this.POINTS_PER_NODE, this.NB_DIMENSION);

        // get leaves of prepare data
        int depth = 2;
        Assert.assertEquals("Test tree depth", depth, getTreeDepth(rTree));
        // MBRLeafNode
        List<RTreeNode> leafNodes = getLeafNode(rTree);
        System.out.println("Number of leaf nodes: " + leafNodes.size());

        int nbLeaf = 2;
        Assert.assertEquals("Test number of leaves", nbLeaf, leafNodes.size());

        // test each leaves
        MBR expPointLeafNode1 = new MBR(this.NB_DIMENSION);
        expPointLeafNode1.addPoint(TestUtil.create2DPoint(1,0));
        expPointLeafNode1.addPoint(TestUtil.create2DPoint(1,2));
        expPointLeafNode1.addPoint(TestUtil.create2DPoint(2,2));
//        Assert.assertEquals("Test leaf 1", expPointLeafNode1, leafNodes.get(0));

        MBR expPointLeafNode2 = new MBR(this.NB_DIMENSION);
        expPointLeafNode2.addPoint(TestUtil.create2DPoint(10,4));
        expPointLeafNode2.addPoint(TestUtil.create2DPoint(-1,5));
        expPointLeafNode2.addPoint(TestUtil.create2DPoint(3,9));
//        Assert.assertEquals("Test leaf 2", expPointLeafNode2, leafNodes.get(1));

        // test MBR values
        Assert.assertEquals("Test MBR of leaf 1", expPointLeafNode1, leafNodes.get(0).getChildNodes().get(0));
        Assert.assertEquals("Test MBR of leaf 2", expPointLeafNode2, leafNodes.get(0).getChildNodes().get(1));

    }

    @Test
    public void testCreate7Points() throws Exception{
        prepareData7Points();
        RTree rTree = createAndPrintTree(this.pointList, this.POINTS_PER_NODE, this.NB_DIMENSION);

        // get leaves of prepare data
        int depth = 2;
        Assert.assertEquals("Test tree depth", depth, getTreeDepth(rTree));
        List<RTreeNode> leafNodes = getLeafNode(rTree);

        int nbLeaf = 3;
        Assert.assertEquals("Test number of leaves", nbLeaf, leafNodes.size());

        // test each leaves
        PointLeafNode expPointLeafNode1 = new PointLeafNode(this.NB_DIMENSION);
        expPointLeafNode1.addPoint(TestUtil.create2DPoint(1,0));
        expPointLeafNode1.addPoint(TestUtil.create2DPoint(1,2));
        expPointLeafNode1.addPoint(TestUtil.create2DPoint(2,2));
        Assert.assertEquals("Test leaf 1", expPointLeafNode1, leafNodes.get(0));


        PointLeafNode expPointLeafNode2 = new PointLeafNode(this.NB_DIMENSION);
        expPointLeafNode2.addPoint(TestUtil.create2DPoint(10,4));
        expPointLeafNode2.addPoint(TestUtil.create2DPoint(-1,5));
        expPointLeafNode2.addPoint(TestUtil.create2DPoint(3,9));
        Assert.assertEquals("Test leaf 2", expPointLeafNode2, leafNodes.get(1));

        PointLeafNode expPointLeafNode3 = new PointLeafNode(this.NB_DIMENSION);
        expPointLeafNode3.addPoint(TestUtil.create2DPoint(11,10));
        Assert.assertEquals("Test leaf 3", expPointLeafNode3, leafNodes.get(2));

        // test MBR values
        Assert.assertEquals("Test MBR of leaf 1", expPointLeafNode1.getMbr(), leafNodes.get(0).getMbr());
        Assert.assertEquals("Test MBR of leaf 2", expPointLeafNode2.getMbr(), leafNodes.get(1).getMbr());
        Assert.assertEquals("Test MBR of leaf 3", expPointLeafNode3.getMbr(), leafNodes.get(2).getMbr());
    }

    @Test
    public void testCreate8Points() throws Exception{
        prepareData8Points();
        RTree rTree = createAndPrintTree(this.pointList, this.POINTS_PER_NODE, this.NB_DIMENSION);

        // get leaves of prepare data
        int depth = 2;
        Assert.assertEquals("Test tree depth", depth, getTreeDepth(rTree));
        List<RTreeNode> leafNodes = getLeafNode(rTree);

        int nbLeaf = 3;
        Assert.assertEquals("Test number of leaves", nbLeaf, leafNodes.size());

        // test each leaves
        PointLeafNode expPointLeafNode1 = new PointLeafNode(this.NB_DIMENSION);
        expPointLeafNode1.addPoint(TestUtil.create2DPoint(1,0));
        expPointLeafNode1.addPoint(TestUtil.create2DPoint(1,2));
        expPointLeafNode1.addPoint(TestUtil.create2DPoint(2,2));
        Assert.assertEquals("Test leaf 1", expPointLeafNode1, leafNodes.get(0));


        PointLeafNode expPointLeafNode2 = new PointLeafNode(this.NB_DIMENSION);
        expPointLeafNode2.addPoint(TestUtil.create2DPoint(10,4));
        expPointLeafNode2.addPoint(TestUtil.create2DPoint(-1,5));
        expPointLeafNode2.addPoint(TestUtil.create2DPoint(3,9));
        Assert.assertEquals("Test leaf 2", expPointLeafNode2, leafNodes.get(1));

        PointLeafNode expPointLeafNode3 = new PointLeafNode(this.NB_DIMENSION);
        expPointLeafNode3.addPoint(TestUtil.create2DPoint(12,-10));
        expPointLeafNode3.addPoint(TestUtil.create2DPoint(11,10));
        Assert.assertEquals("Test leaf 3", expPointLeafNode3, leafNodes.get(2));

        // test MBR values
        Assert.assertEquals("Test MBR of leaf 1", expPointLeafNode1.getMbr(), leafNodes.get(0).getMbr());
        Assert.assertEquals("Test MBR of leaf 2", expPointLeafNode2.getMbr(), leafNodes.get(1).getMbr());
        Assert.assertEquals("Test MBR of leaf 3", expPointLeafNode3.getMbr(), leafNodes.get(2).getMbr());
    }

    @Test
    public void testCreate9Points() throws Exception{
        prepareData9Points();
        RTree rTree = createAndPrintTree(this.pointList, this.POINTS_PER_NODE, this.NB_DIMENSION);

        // get leaves of prepare data
        int depth = 2;
        Assert.assertEquals("Test tree depth", depth, getTreeDepth(rTree));
        List<RTreeNode> leafNodes = getLeafNode(rTree);

        int nbLeaf = 3;
        Assert.assertEquals("Test number of leaves", nbLeaf, leafNodes.size());

        // test each leaves
        PointLeafNode expPointLeafNode1 = new PointLeafNode(this.NB_DIMENSION);
        expPointLeafNode1.addPoint(TestUtil.create2DPoint(1,0));
        expPointLeafNode1.addPoint(TestUtil.create2DPoint(1,2));
        expPointLeafNode1.addPoint(TestUtil.create2DPoint(2,2));
        Assert.assertEquals("Test leaf 1", expPointLeafNode1, leafNodes.get(0));


        PointLeafNode expPointLeafNode2 = new PointLeafNode(this.NB_DIMENSION);
        expPointLeafNode2.addPoint(TestUtil.create2DPoint(10,4));
        expPointLeafNode2.addPoint(TestUtil.create2DPoint(-1,5));
        expPointLeafNode2.addPoint(TestUtil.create2DPoint(3,9));
        Assert.assertEquals("Test leaf 2", expPointLeafNode2, leafNodes.get(1));

        PointLeafNode expPointLeafNode3 = new PointLeafNode(this.NB_DIMENSION);
        expPointLeafNode3.addPoint(TestUtil.create2DPoint(12,-10));
        expPointLeafNode3.addPoint(TestUtil.create2DPoint(11,10));
        expPointLeafNode3.addPoint(TestUtil.create2DPoint(11,11));
        Assert.assertEquals("Test leaf 3", expPointLeafNode3, leafNodes.get(2));

//        PointLeafNode expLeafNode4 = new PointLeafNode(this.NB_DIMENSION);
//        expLeafNode4.addPoint(TestUtil.create2DPoint(15,8));
//        Assert.assertEquals("Test leaf 4", expLeafNode4, leafNodes.get(3));

        // test MBR values
        Assert.assertEquals("Test MBR of leaf 1", expPointLeafNode1.getMbr(), leafNodes.get(0).getMbr());
        Assert.assertEquals("Test MBR of leaf 2", expPointLeafNode2.getMbr(), leafNodes.get(1).getMbr());
        Assert.assertEquals("Test MBR of leaf 3", expPointLeafNode3.getMbr(), leafNodes.get(2).getMbr());
//        Assert.assertEquals("Test MBR of leaf 4", expLeafNode4.getMbr(), leafNodes.get(3).getMbr());
    }

    @Test
    public void testCreate10Points() throws Exception{
        prepareData10Points();
        RTree rTree = createAndPrintTree(this.pointList, this.POINTS_PER_NODE, this.NB_DIMENSION);

        // get leaves of prepare data
        int depth = 3;
        Assert.assertEquals("Test tree depth", depth, getTreeDepth(rTree));
        List<RTreeNode> leafNodes = getLeafNode(rTree);

        int nbLeaf = 4;
        Assert.assertEquals("Test number of leaves", nbLeaf, leafNodes.size());

        // test each leaves
        PointLeafNode expPointLeafNode1 = new PointLeafNode(this.NB_DIMENSION);
        expPointLeafNode1.addPoint(TestUtil.create2DPoint(1,0));
        expPointLeafNode1.addPoint(TestUtil.create2DPoint(1,2));
        expPointLeafNode1.addPoint(TestUtil.create2DPoint(2,2));
        Assert.assertEquals("Test leaf 1", expPointLeafNode1, leafNodes.get(0));


        PointLeafNode expPointLeafNode2 = new PointLeafNode(this.NB_DIMENSION);
        expPointLeafNode2.addPoint(TestUtil.create2DPoint(10,4));
        expPointLeafNode2.addPoint(TestUtil.create2DPoint(-1,5));
        expPointLeafNode2.addPoint(TestUtil.create2DPoint(3,9));
        Assert.assertEquals("Test leaf 2", expPointLeafNode2, leafNodes.get(1));

        PointLeafNode expPointLeafNode3 = new PointLeafNode(this.NB_DIMENSION);
        expPointLeafNode3.addPoint(TestUtil.create2DPoint(12,-10));
        expPointLeafNode3.addPoint(TestUtil.create2DPoint(15,8));
        expPointLeafNode3.addPoint(TestUtil.create2DPoint(11,10));
        Assert.assertEquals("Test leaf 3", expPointLeafNode3, leafNodes.get(2));

        PointLeafNode expPointLeafNode4 = new PointLeafNode(this.NB_DIMENSION);
        expPointLeafNode4.addPoint(TestUtil.create2DPoint(11,11));
        Assert.assertEquals("Test leaf 4", expPointLeafNode4, leafNodes.get(3));

//        Assert.assertEquals("Test leaf 3", expPointLeafNode3, leafNodes.get(2));

        // test MBR values
        Assert.assertEquals("Test MBR of leaf 1", expPointLeafNode1.getMbr(), leafNodes.get(0).getMbr());
        Assert.assertEquals("Test MBR of leaf 2", expPointLeafNode2.getMbr(), leafNodes.get(1).getMbr());
        Assert.assertEquals("Test MBR of leaf 3", expPointLeafNode3.getMbr(), leafNodes.get(2).getMbr());
        Assert.assertEquals("Test MBR of leaf 4", expPointLeafNode4.getMbr(), leafNodes.get(3).getMbr());
    }



    private List<RTreeNode> getLeafNode(RTree tree){
        List<RTreeNode> nodes = new ArrayList<RTreeNode>();
        nodes.add(tree.getRootNode());
        List<RTreeNode> parentsNodes = new ArrayList<RTreeNode>();
        int depth = 0;
        do {
            parentsNodes.clear();
            parentsNodes.addAll(nodes);
            nodes.clear();
            for (int i = 0; i < parentsNodes.size(); i++) {
                nodes.addAll(parentsNodes.get(i).getChildNodes());
            }
        } while (!nodes.isEmpty() && (!nodes.get(0).isLeaf()));
        return nodes;
    }

    private int getTreeDepth(RTree tree){
        List<RTreeNode> nodes = new ArrayList<RTreeNode>();
        nodes.add(tree.getRootNode());
        List<RTreeNode> parentsNodes = new ArrayList<RTreeNode>();
        int depth = 0;
        while(!nodes.isEmpty()){
            parentsNodes.clear();
            parentsNodes.addAll(nodes);
            nodes.clear();
            for(int i =0; i<parentsNodes.size(); i++){
                List<RTreeNode> childNodes = parentsNodes.get(i).getChildNodes();
                if(childNodes != null){
                    nodes.addAll(childNodes);
                }
            }
            depth++;
        }
        return depth;
    }


    private List<RTreeNode> getTreeNodes(RTree tree, int level){
        List<RTreeNode> nodes = new ArrayList<RTreeNode>();
        if(level == 0){
            RTreeNode root = tree.getRootNode();
            nodes.add(root);
            return nodes;
        }
        int count = 0;
        List<RTreeNode> parentsNodes = new ArrayList<RTreeNode>();
        nodes.add(tree.getRootNode());
        while(count < level){
            parentsNodes.clear();
            parentsNodes.addAll(nodes);
            nodes.clear();
            for(int i =0; i< parentsNodes.size(); i++){
                nodes.addAll(parentsNodes.get(i).getChildNodes());
            }
            count++;
        }
        return nodes;
    }



    @Test
    public void testSearch2D() throws Exception {
        // Prepare data
        Log log = LogFactory.getLog(this.getClass());
        log.warn("Test create tree 2D");
        prepareData();

        STRPacking str = new STRPacking(this.POINTS_PER_NODE, this.NB_DIMENSION);
        RTree rTree = str.createRTree(pointList);

        System.out.println("Test createCreateTree2D");
        System.out.println("--Input points: ");
        for(int i =0; i<this.pointList.size(); i++){
            System.out.println(this.pointList.get(i));
        }

        System.out.println("--Result tree: ");
        System.out.println(rTree.toString());

        System.out.println("--Search (3,9): ");
        List<RTreeNode> searchNodes = rTree.search(TestUtil.create2DPoint(3,9));
        for(int i =0; i<searchNodes.size(); i++){
            System.out.println(searchNodes.get(i));
        }
        Assert.assertTrue("Result should return no values", !searchNodes.isEmpty());

        System.out.println("--Search (10,10): ");
        searchNodes = rTree.search(TestUtil.create2DPoint(10,10));
        for(int i =0; i<searchNodes.size(); i++){
            System.out.println(searchNodes.get(i));
        }
        Assert.assertTrue("Result should return no values", searchNodes.isEmpty());

        System.out.println("--Search (10,5): ");
        searchNodes = rTree.search(TestUtil.create2DPoint(10,5));
        for(int i =0; i<searchNodes.size(); i++){
            System.out.println(searchNodes.get(i));
        }
        Assert.assertTrue("Result should return no values", searchNodes.isEmpty());

        System.out.println("--Search (10,15): ");
        searchNodes = rTree.search(TestUtil.create2DPoint(10,15));
        for(int i =0; i<searchNodes.size(); i++){
            System.out.println(searchNodes.get(i));
        }
        Assert.assertTrue("Result should return no values", searchNodes.isEmpty());
    }

}
