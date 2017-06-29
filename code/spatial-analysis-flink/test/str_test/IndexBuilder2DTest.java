package str_test;

import de.tu_berlin.dima.IndexBuilder;
import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.STRPartitioner;
import de.tu_berlin.dima.datatype.*;
import de.tu_berlin.dima.serializer.*;
import de.tu_berlin.dima.test.IndexBuilderResult;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Suite;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by JML on 5/19/17.
 */
@Suite.SuiteClasses(IndexBuilder2DTest.class)
public class IndexBuilder2DTest {
    private static final int POINTS_PER_NODE = 3;
    private static final int NB_DIMENSION = 2;

    private List<Point> points = new ArrayList<Point>();
    private List<Point> samplePoints = new ArrayList<Point>();

    private IndexBuilder indexBuilder = new IndexBuilder();

    private void prepareData(){
        this.points.clear();
        points.add(TestUtil.create2DPoint(1, 0));
        points.add(TestUtil.create2DPoint(1, 2));
        points.add(TestUtil.create2DPoint(2, 2));
        points.add(TestUtil.create2DPoint(3, 9));
        points.add(TestUtil.create2DPoint(10, 4));
        points.add(TestUtil.create2DPoint(-1, 5));
        points.add(TestUtil.create2DPoint(11, 10));
    }


    // What to test
    // 1. Test STRPartitioner
    // 2. Test updating boundaries when we do the partitioning
    // 3. Test local RTree
    // 4. Test the global RTree

    // TODO: test with sample size = 1

    @Test
    public void testBuildIndex() throws Exception {

        prepareData();
        samplePoints = points.subList(0, points.size()/2);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        int parallelism = env.getParallelism();
        double sampleRate = 0.8;

        DataSet<Point> pointDS = env.fromCollection(points);
        DataSet<Point> samplePointDS = env.fromCollection(samplePoints);

        IndexBuilderResult result = indexBuilder.buildIndexTestVersion(pointDS, pointDS, NB_DIMENSION, POINTS_PER_NODE, sampleRate, parallelism);
//        indexBuilder.buildIndex(pointDS, NB_DIMENSION, POINTS_PER_NODE, sampleRate, parallelism);

        RTree globalTree = result.getGlobalRTree().collect().get(0);
        System.out.println("Global tree: " + globalTree.toString());
        System.out.println("Local tree: ");
        result.getLocalRTree().print();
        System.out.println(result.getLocalRTree().count());
    }

    @Test
    public void testPartition() throws Exception{
        prepareData();
        samplePoints = points.subList(0, points.size()/2);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.registerTypeWithKryoSerializer(Point.class, PointSerializer.class);
        env.registerTypeWithKryoSerializer(MBR.class, MBRSerializer.class);
        env.registerTypeWithKryoSerializer(PartitionedMBR.class, PartitionedMBRSerializer.class);
        env.registerTypeWithKryoSerializer(LeafNode.class, RTreeNodeSerializer.class);
        env.registerTypeWithKryoSerializer(PointLeafNode.class, RTreeNodeSerializer.class);
        env.registerTypeWithKryoSerializer(MBRLeafNode.class, RTreeNodeSerializer.class);
        env.registerTypeWithKryoSerializer(NonLeafNode.class, RTreeNodeSerializer.class);
        env.registerTypeWithKryoSerializer(RTreeNode.class, RTreeNodeSerializer.class);
        env.registerTypeWithKryoSerializer(RTree.class, RTreeSerializer.class);

        int parallelism = env.getParallelism();
        double sampleRate = 0.8;

        DataSet<Point> pointDS = env.fromCollection(points);
        DataSet<Point> samplePointDS = env.fromCollection(samplePoints);
        pointDS.mapPartition(new MapPartitionFunction<Point, String>() {
            @Override
            public void mapPartition(Iterable<Point> iterable, Collector<String> collector) throws Exception {
                Iterator<Point> pointIter = iterable.iterator();
                String result = "beforePartition ";
                while(pointIter.hasNext()){
                    result = result + pointIter.next().toString();
                }
                collector.collect(result);

            }
        }).print();

        IndexBuilderResult result = indexBuilder.buildIndexTestVersion(pointDS, pointDS, NB_DIMENSION, POINTS_PER_NODE, sampleRate, parallelism);
//        indexBuilder.buildIndex(pointDS, NB_DIMENSION, POINTS_PER_NODE, sampleRate, parallelism);

        result.getData().mapPartition(new MapPartitionFunction<Point, String>() {
            @Override
            public void mapPartition(Iterable<Point> iterable, Collector<String> collector) throws Exception {
                Iterator<Point> pointIter = iterable.iterator();
                String result = "partitionedData ";
                while(pointIter.hasNext()){
                    result = result + pointIter.next().toString();
                }
                collector.collect(result);
            }
        }).print();

        RTree globalTree = result.getGlobalRTree().collect().get(0);
        System.out.println("Global tree: " + globalTree.toString());
        System.out.println("Local tree: ");
        result.getLocalRTree().print();
        System.out.println("Total memory: " + globalTree.getNumBytes());
    }

    @Test
    public void testCreateSTRPartitioner() throws Exception {
        prepareData();
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        int paralleism = env.getParallelism();
        double sampleRate = 0.5;

        DataSet<Point> pointDS = env.fromCollection(points);

        STRPartitioner partitioner = indexBuilder.createSTRPartitionerTestVersion(pointDS, pointDS, NB_DIMENSION, POINTS_PER_NODE, sampleRate, paralleism);
        String expect = "< [0, MBR: Min(-1.0,0.0) - Max(2.0,2.0) ][1, MBR: Min(-1.0,2.0) - Max(2.0,10.0) ] , [2, MBR: Min(2.0,0.0) - Max(11.0,9.0) ][3, MBR: Min(2.0,9.0) - Max(11.0,10.0) ] ,  > \n";
        String actual = partitioner.getrTree().toString();
        Assert.assertEquals("Boundaries of RTree in partitioner is incorrect", actual, expect);
        Assert.assertEquals("Size of Rtree", 7, partitioner.getrTree().getRootNode().getSize());

        samplePoints.add(TestUtil.create2DPoint(1, 2));
        samplePoints.add(TestUtil.create2DPoint(2, 2));
        samplePoints.add(TestUtil.create2DPoint(10, 4));
        samplePoints.add(TestUtil.create2DPoint(-1, 5));

        DataSet<Point> samplePointDS = env.fromCollection(samplePoints);
        STRPartitioner partitioner2 = indexBuilder.createSTRPartitionerTestVersion(pointDS, samplePointDS, NB_DIMENSION, POINTS_PER_NODE, sampleRate, paralleism);
        String expect2 = "< [0, MBR: Min(-1.0,0.0) - Max(1.0,2.0) ][1, MBR: Min(-1.0,2.0) - Max(1.0,10.0) ] , [2, MBR: Min(1.0,0.0) - Max(11.0,2.0) ][3, MBR: Min(1.0,2.0) - Max(11.0,10.0) ] ,  > \n";
        String actual2 = partitioner2.getrTree().toString();
        Assert.assertEquals("Boundaries of RTree in partitioner (sample data) is incorrect", actual2, expect2);

        // Check MBR of first node level
        List<RTreeNode> firstLevelNodes = partitioner.getrTree().getRootNode().getChildNodes();
        Assert.assertEquals("There should be only two 1st level nodes", 2, firstLevelNodes.size());
        String firstNode = "[0, MBR: Min(-1.0,0.0) - Max(2.0,2.0) ][1, MBR: Min(-1.0,2.0) - Max(2.0,10.0) ]";
        String firstMBR = "MBR: Min(-1.0,0.0) - Max(2.0,10.0)";
        Assert.assertEquals("First node", firstNode, firstLevelNodes.get(0).toString());
        Assert.assertEquals("MBR of first node", firstMBR, firstLevelNodes.get(0).getMbr().toString());

        String secondNode = "[2, MBR: Min(2.0,0.0) - Max(11.0,9.0) ][3, MBR: Min(2.0,9.0) - Max(11.0,10.0) ]";
        String secondMBR = "MBR: Min(2.0,0.0) - Max(11.0,10.0)";
        Assert.assertEquals("Second node", secondNode, firstLevelNodes.get(1).toString());
        Assert.assertEquals("MBR of second node", secondMBR, firstLevelNodes.get(1).getMbr().toString());

    }

    // test parttitioner
    @Test
    public void testSearchSTRPartitioner() throws Exception {
        prepareData();
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        int paralleism = env.getParallelism();
        double sampleRate = 0.5;

        DataSet<Point> pointDS = env.fromCollection(points);

        STRPartitioner partitioner = indexBuilder.createSTRPartitionerTestVersion(pointDS, pointDS, NB_DIMENSION, POINTS_PER_NODE, sampleRate, paralleism);
        String expect = "< [0, MBR: Min(-1.0,0.0) - Max(2.0,2.0) ][1, MBR: Min(-1.0,2.0) - Max(2.0,10.0) ] , [2, MBR: Min(2.0,0.0) - Max(11.0,9.0) ][3, MBR: Min(2.0,9.0) - Max(11.0,10.0) ] ,  > \n";
        String actual = partitioner.getrTree().toString();
        Assert.assertEquals("Boundaries of RTree in partitioner is incorrect", actual, expect);

        RTree rTree = partitioner.getrTree();
        List<RTreeNode> result = rTree.search(TestUtil.create2DPoint(-1, 5));
        String expectedMBR = "[[0, MBR: Min(-1.0,0.0) - Max(2.0,2.0) ][1, MBR: Min(-1.0,2.0) - Max(2.0,10.0) ]]";
        Assert.assertEquals("Test (-1,5)", expectedMBR, result.toString());
        System.out.println(result);

        result = rTree.search(TestUtil.create2DPoint(0, 1));
        expectedMBR = "[[0, MBR: Min(-1.0,0.0) - Max(2.0,2.0) ][1, MBR: Min(-1.0,2.0) - Max(2.0,10.0) ]]";
        Assert.assertEquals("Test (0,1)", expectedMBR, result.toString());
        System.out.println(result);

        result = rTree.search(TestUtil.create2DPoint(2, 2));
        expectedMBR = "[[0, MBR: Min(-1.0,0.0) - Max(2.0,2.0) ][1, MBR: Min(-1.0,2.0) - Max(2.0,10.0) ], [2, MBR: Min(2.0,0.0) - Max(11.0,9.0) ][3, MBR: Min(2.0,9.0) - Max(11.0,10.0) ]]";
        Assert.assertEquals("Test (2,2)", expectedMBR, result.toString());
        System.out.println(result);

        result = rTree.search(TestUtil.create2DPoint(2, 100));
        expectedMBR = "[]";
        Assert.assertEquals("Test (2,100)", expectedMBR, result.toString());
        System.out.println(result);

        result = rTree.search(TestUtil.create2DPoint(10, 15));
        expectedMBR = "[]";
        Assert.assertEquals("Test (10,15)", expectedMBR, result.toString());
        System.out.println(result);


    }

        // test create global tree

    // test create local rtree
    @Test
    public void testCreateLocalTree() throws Exception {
        prepareData();
        IndexBuilder indexBuilder = new IndexBuilder();
        RTree rTree = indexBuilder.createLocalRTree(this.points, NB_DIMENSION, POINTS_PER_NODE);
        String expect = "< < (1.0,0.0)(1.0,2.0) , (2.0,2.0)(-1.0,5.0) ,  > \n" +
                " , < (10.0,4.0)(3.0,9.0) , (11.0,10.0) ,  > \n" +
                " ,  > \n";
        Assert.assertEquals("Local rtree", expect, rTree.toString());

        Assert.assertEquals("Size of root node", 7, rTree.getRootNode().getSize());

        List<RTreeNode> secondLevelNodes   = rTree.getRootNode().getChildNodes();
        List<RTreeNode> firstLevelNodes = new ArrayList<RTreeNode>();
        for (RTreeNode node: secondLevelNodes) {
            firstLevelNodes.addAll(node.getChildNodes());
        }
        String firstNode = "(1.0,0.0)(1.0,2.0)";
        String firstMBR = "MBR: Min(1.0,0.0) - Max(1.0,2.0)";
        Assert.assertEquals("First node", firstNode, firstLevelNodes.get(0).toString());
        Assert.assertEquals("MBR of first node", firstMBR, firstLevelNodes.get(0).getMbr().toString());

        String secondNode = "(2.0,2.0)(-1.0,5.0)";
        String secondMBR = "MBR: Min(-1.0,2.0) - Max(2.0,5.0)";
        Assert.assertEquals("Second node", secondNode, firstLevelNodes.get(1).toString());
        Assert.assertEquals("MBR of second node", secondMBR, firstLevelNodes.get(1).getMbr().toString());

        String thirdNode = "(10.0,4.0)(3.0,9.0)";
        String thirdMBR = "MBR: Min(3.0,4.0) - Max(10.0,9.0)";
        Assert.assertEquals("Third node", thirdNode, firstLevelNodes.get(2).toString());
        Assert.assertEquals("MBR of third node", thirdMBR, firstLevelNodes.get(2).getMbr().toString());

        String fourthNode = "(11.0,10.0)";
        String fourthMBR = "MBR: Min(11.0,10.0) - Max(11.0,10.0)";
        Assert.assertEquals("Fourth node", fourthNode, firstLevelNodes.get(3).toString());
        Assert.assertEquals("MBR of fourth node", fourthMBR, firstLevelNodes.get(3).getMbr().toString());


        String firstNodeLv2 = "< (1.0,0.0)(1.0,2.0) , (2.0,2.0)(-1.0,5.0) ,  > \n";
        String firstMBRLv2 = "MBR: Min(-1.0,0.0) - Max(2.0,5.0)";
        Assert.assertEquals("First node Level 2", firstNodeLv2, secondLevelNodes.get(0).toString());
        Assert.assertEquals("MBR of first node Level 2", firstMBRLv2, secondLevelNodes.get(0).getMbr().toString());

        String secondNodeLv2 = "< (10.0,4.0)(3.0,9.0) , (11.0,10.0) ,  > \n";
        String secondMBRLv2 = "MBR: Min(3.0,4.0) - Max(11.0,10.0)";
        Assert.assertEquals("Second node Level 2", secondNodeLv2, secondLevelNodes.get(1).toString());
        Assert.assertEquals("MBR of second node level 2", secondMBRLv2, secondLevelNodes.get(1).getMbr().toString());

        System.out.println(rTree.toString());
    }

}