package str_test;

import de.tu_berlin.dima.IndexBuilder;
import de.tu_berlin.dima.OperationExecutor;
import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.datatype.MBR;
import de.tu_berlin.dima.datatype.Point;
import de.tu_berlin.dima.test.IndexBuilderResult;
import de.tu_berlin.dima.util.Utils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Suite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by JML on 6/27/17.
 */
@Suite.SuiteClasses(OperationExecutorTest.class)
public class OperationExecutorTest {
    private static final int POINTS_PER_NODE = 3;
    private static final int NB_DIMENSION = 2;

    private List<Point> points = new ArrayList<Point>();
    private List<Point> samplePoints = new ArrayList<Point>();

    private IndexBuilder indexBuilder = new IndexBuilder();
    private OperationExecutor operationExecutor = new OperationExecutor();

    private DataSet<RTree> globalTree;
    private DataSet<RTree> localTrees;
    private DataSet<Point> partitionedData;

    private void prepareData() throws Exception {
        this.points.clear();
        points.add(TestUtil.create2DPoint(1, 0));
        points.add(TestUtil.create2DPoint(1, 2));
        points.add(TestUtil.create2DPoint(2, 2));
        points.add(TestUtil.create2DPoint(3, 9));
        points.add(TestUtil.create2DPoint(10, 4));
        points.add(TestUtil.create2DPoint(-1, 5));
        points.add(TestUtil.create2DPoint(11, 10));

        this.samplePoints = points.subList(0, points.size()/2);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        int parallelism = env.getParallelism();
        double sampleRate = 0.8;

        DataSet<Point> pointDS = env.fromCollection(this.points);
        DataSet<Point> samplePointDS = env.fromCollection(this.samplePoints);
        Utils.registerCustomSerializer(env);

        IndexBuilderResult indexResult = this.indexBuilder.buildIndexTestVersion(pointDS, pointDS, NB_DIMENSION, POINTS_PER_NODE, sampleRate, parallelism);

        System.out.println("Global tree: " + indexResult.getGlobalRTree().toString());
        System.out.println("Local tree: ");
        indexResult.getLocalRTree().print();

        partitionedData = indexResult.getData();
        globalTree = indexResult.getGlobalRTree();
        localTrees = indexResult.getLocalRTree();
    }

    public static void main(String[] args) throws Exception {
        OperationExecutorTest test = new OperationExecutorTest();
        test.testBoxRangeQuery();
    }

    @Test
    public void testBoxRangeQuery() throws Exception {
        prepareData();

        Point p1 = TestUtil.create2DPoint(-1,-1);
        Point p2 = TestUtil.create2DPoint(10,4);
        MBR mbr = new MBR(2);
        mbr.addPoint(p1);
        mbr.addPoint(p2);

        // Test found result
        List<Point> actual = this.operationExecutor.boxRangeQuery(mbr, this.partitionedData, this.globalTree).collect();
        List<Point> expected = new ArrayList<Point>();
        expected.add(TestUtil.create2DPoint(1, 0));
        expected.add(TestUtil.create2DPoint(1, 2));
        expected.add(TestUtil.create2DPoint(2, 2));
        expected.add(TestUtil.create2DPoint(10, 4));

        for(int i =0; i< expected.size(); i++){
            Assert.assertEquals("Not found point: " + expected.get(i) , true, actual.contains(expected.get(i)));
        }

        // Test not found result
        MBR mbr2 = new MBR(TestUtil.create2DPoint(0,0), TestUtil.create2DPoint(0.5f, 0.5f));
        List<Point> actual2 = this.operationExecutor.boxRangeQuery(mbr2, this.partitionedData, this.globalTree).collect();
        Assert.assertEquals("Not empty result", true, actual2.isEmpty());
    }

    @Test
    public void testkNNQuery() throws Exception{
        prepareData();
        samplePoints = points.subList(0, points.size()/2);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        int parallelism = env.getParallelism();
        double sampleRate = 0.8;

        DataSet<Point> pointDS = env.fromCollection(points);
        DataSet<Point> samplePointDS = env.fromCollection(samplePoints);

        IndexBuilderResult indexResult = indexBuilder.buildIndexTestVersion(pointDS, pointDS, NB_DIMENSION, POINTS_PER_NODE, sampleRate, parallelism);
        DataSet<Point> partitionedData = indexResult.getData();
        DataSet<RTree> globalTree = indexResult.getGlobalRTree();
        DataSet<RTree> localTrees = indexResult.getLocalRTree();

        globalTree.printOnTaskManager("x");
        localTrees.printOnTaskManager("xxx");

        Point p1 = TestUtil.create2DPoint(-1,-1);

        DataSet<Point> result = this.operationExecutor.kNNQuery(p1, 4, partitionedData);
//        result.print();
        result.printOnTaskManager("x");
//        System.out.println(env.getExecutionPlan());
        List<Point> actual = result.collect();

        List<Point> expected = new ArrayList<Point>();
        expected.add(TestUtil.create2DPoint(1, 0));
        expected.add(TestUtil.create2DPoint(1, 2));
        expected.add(TestUtil.create2DPoint(2, 2));
        expected.add(TestUtil.create2DPoint(-1, 5));

        for(int i =0; i< expected.size(); i++){
            Assert.assertEquals("Not found point: " + expected.get(i) , true, actual.contains(expected.get(i)));
        }

        Map<String, List<Point>> expectedMap = new HashMap<String, List<Point>>();
        List<Point> expected10 = new ArrayList<Point>();
        expected10.add(TestUtil.create2DPoint(1,0));
        expected10.add(TestUtil.create2DPoint(1,2));
        expected10.add(TestUtil.create2DPoint(2,2));
        expectedMap.put(TestUtil.create2DPoint(1,0).toString(),expected10);

        List<Point> expected12 = new ArrayList<Point>();
        expected12.add(TestUtil.create2DPoint(1,2));
        expected12.add(TestUtil.create2DPoint(1,0));
        expected12.add(TestUtil.create2DPoint(2,2));
        expectedMap.put(TestUtil.create2DPoint(1,2).toString(), expected12);

        List<Point> expected22 = new ArrayList<Point>();
        expected22.add(TestUtil.create2DPoint(2,2));
        expected22.add(TestUtil.create2DPoint(1,0));
        expected22.add(TestUtil.create2DPoint(1,2));
        expectedMap.put(TestUtil.create2DPoint(2,2).toString(), expected22);

        List<Point> expected39 = new ArrayList<Point>();
        expected39.add(TestUtil.create2DPoint(3,9));
        expected39.add(TestUtil.create2DPoint(-1,5));
        expected39.add(TestUtil.create2DPoint(2,2));
        expectedMap.put(TestUtil.create2DPoint(3,9).toString(), expected39);

        List<Point> expected15 = new ArrayList<Point>();
        expected15.add(TestUtil.create2DPoint(-1,5));
        expected15.add(TestUtil.create2DPoint(1,2));
        expected15.add(TestUtil.create2DPoint(2,2));
        expectedMap.put(TestUtil.create2DPoint(-1,5).toString(), expected15);

        List<Point> expected104 = new ArrayList<Point>();
        expected104.add(TestUtil.create2DPoint(10,4));
        expected104.add(TestUtil.create2DPoint(2,2));
        expected104.add(TestUtil.create2DPoint(11,10));
        expectedMap.put(TestUtil.create2DPoint(10,4).toString(), expected104);

        List<Point> expected1110 = new ArrayList<Point>();
        expected1110.add(TestUtil.create2DPoint(11,10));
        expected1110.add(TestUtil.create2DPoint(10,4));
        expected1110.add(TestUtil.create2DPoint(3,9));
        expectedMap.put(TestUtil.create2DPoint(11,10).toString(), expected1110);

        for(int i =0; i<this.points.size(); i++){
            Point point = this.points.get(i);
            List<Point> actualList = this.operationExecutor.kNNQuery(point, 3, partitionedData).collect();
            List<Point> expectedList = expectedMap.get(point.toString());
            System.out.println(expectedList);
            for(int j =0; j<actualList.size(); j++){
                Assert.assertEquals("Not found point in actual list: " + expectedList.get(j) , true, actualList.contains(expectedList.get(j)));
            }
            for(int j =0; j<actualList.size(); j++){
                Assert.assertEquals("Not found point in expected list: " + actualList.get(j) , true, expectedList.contains(actualList.get(j)));
            }
        }
    }


    @Test
    public void testkNNJoin() throws Exception{
        prepareData();
        samplePoints = points.subList(0, points.size()/2);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        int parallelism = env.getParallelism();
        double sampleRate = 0.8;

        DataSet<Point> pointDS = env.fromCollection(points);
        DataSet<Point> pointDS2 = env.fromCollection(points);

        IndexBuilderResult indexResult = indexBuilder.buildIndexTestVersion(pointDS, pointDS, NB_DIMENSION, POINTS_PER_NODE, sampleRate, parallelism);
        DataSet<RTree> globalTree = indexResult.getGlobalRTree();
        DataSet<RTree> localTrees = indexResult.getLocalRTree();

        globalTree.printOnTaskManager("x");
        localTrees.printOnTaskManager("xxx");

        List<Tuple2<Point, Point>> result = this.operationExecutor.kNNJoin(3, 2, pointDS, pointDS2, POINTS_PER_NODE, sampleRate, parallelism).collect();

        Map<String, List<Point>> expectedMap = new HashMap<String, List<Point>>();
        List<Point> expected10 = new ArrayList<Point>();
        expected10.add(TestUtil.create2DPoint(1,0));
        expected10.add(TestUtil.create2DPoint(1,2));
        expected10.add(TestUtil.create2DPoint(2,2));
        expectedMap.put(TestUtil.create2DPoint(1,0).toString(),expected10);

        List<Point> expected12 = new ArrayList<Point>();
        expected12.add(TestUtil.create2DPoint(1,2));
        expected12.add(TestUtil.create2DPoint(1,0));
        expected12.add(TestUtil.create2DPoint(2,2));
        expectedMap.put(TestUtil.create2DPoint(1,2).toString(), expected12);

        List<Point> expected22 = new ArrayList<Point>();
        expected22.add(TestUtil.create2DPoint(2,2));
        expected22.add(TestUtil.create2DPoint(1,0));
        expected22.add(TestUtil.create2DPoint(1,2));
        expectedMap.put(TestUtil.create2DPoint(2,2).toString(), expected22);

        List<Point> expected39 = new ArrayList<Point>();
        expected39.add(TestUtil.create2DPoint(3,9));
        expected39.add(TestUtil.create2DPoint(-1,5));
        expected39.add(TestUtil.create2DPoint(2,2));
        expectedMap.put(TestUtil.create2DPoint(3,9).toString(), expected39);

        List<Point> expected15 = new ArrayList<Point>();
        expected15.add(TestUtil.create2DPoint(-1,5));
        expected15.add(TestUtil.create2DPoint(1,2));
        expected15.add(TestUtil.create2DPoint(2,2));
        expectedMap.put(TestUtil.create2DPoint(-1,5).toString(), expected15);

        List<Point> expected104 = new ArrayList<Point>();
        expected104.add(TestUtil.create2DPoint(10,4));
        expected104.add(TestUtil.create2DPoint(2,2));
        expected104.add(TestUtil.create2DPoint(11,10));
        expectedMap.put(TestUtil.create2DPoint(10,4).toString(), expected104);

        List<Point> expected1110 = new ArrayList<Point>();
        expected1110.add(TestUtil.create2DPoint(11,10));
        expected1110.add(TestUtil.create2DPoint(10,4));
        expected1110.add(TestUtil.create2DPoint(3,9));
        expectedMap.put(TestUtil.create2DPoint(11,10).toString(), expected1110);

        for(int i =0; i<result.size(); i++){
            Point left = result.get(i).f0;
            Point right = result.get(i).f1;
            List<Point> expectedList = expectedMap.get(left.toString());
            Assert.assertEquals("Not found point in expected list: " + left , true, expectedList.contains(right));
        }
    }

    @Test
    public void testCircleRange() throws Exception {
        prepareData();

        Point queryPoint = TestUtil.create2DPoint(1,2);
        float radius = 5f;
        // Test found result
        List<Point> actual = this.operationExecutor.circleRangeQuery(queryPoint, radius, this.partitionedData, this.globalTree).collect();
        List<Point> expected = new ArrayList<Point>();
        expected.add(TestUtil.create2DPoint(1, 0));
        expected.add(TestUtil.create2DPoint(1, 2));
        expected.add(TestUtil.create2DPoint(2, 2));
        expected.add(TestUtil.create2DPoint(-1,5));

        System.out.println(actual);
        for(int i =0; i< expected.size(); i++){
            Assert.assertEquals("Not found point: " + expected.get(i) , true, actual.contains(expected.get(i)));
        }
        Assert.assertEquals(expected.size(), actual.size());

        // Test not found result
        Point notFoundPoint = TestUtil.create2DPoint(3,4);
        float notFoundRadius = 1;
        List<Point> actual2 = this.operationExecutor.circleRangeQuery(notFoundPoint, notFoundRadius, this.partitionedData, this.globalTree).collect();
        Assert.assertEquals("Not empty result", true, actual2.isEmpty());


        Map<String, List<Point>> expectedMap = new HashMap<String, List<Point>>();
        List<Point> expected10 = new ArrayList<Point>();
        expected10.add(TestUtil.create2DPoint(1,0));
        expected10.add(TestUtil.create2DPoint(1,2));
        expected10.add(TestUtil.create2DPoint(2,2));
        expected10.add(TestUtil.create2DPoint(-1,5));
        expectedMap.put(TestUtil.create2DPoint(1,0).toString(),expected10);

        List<Point> expected12 = new ArrayList<Point>();
        expected12.add(TestUtil.create2DPoint(1,2));
        expected12.add(TestUtil.create2DPoint(1,0));
        expected12.add(TestUtil.create2DPoint(2,2));
        expected12.add(TestUtil.create2DPoint(-1,5));
        expectedMap.put(TestUtil.create2DPoint(1,2).toString(), expected12);

        List<Point> expected22 = new ArrayList<Point>();
        expected22.add(TestUtil.create2DPoint(2,2));
        expected22.add(TestUtil.create2DPoint(1,0));
        expected22.add(TestUtil.create2DPoint(1,2));
        expected22.add(TestUtil.create2DPoint(-1,5));
        expectedMap.put(TestUtil.create2DPoint(2,2).toString(), expected22);

        List<Point> expected39 = new ArrayList<Point>();
        expected39.add(TestUtil.create2DPoint(3,9));
        expected39.add(TestUtil.create2DPoint(-1,5));
        expected39.add(TestUtil.create2DPoint(2,2));
        expectedMap.put(TestUtil.create2DPoint(3,9).toString(), expected39);

        List<Point> expected15 = new ArrayList<Point>();
        expected15.add(TestUtil.create2DPoint(-1,5));
        expected15.add(TestUtil.create2DPoint(1,2));
        expected15.add(TestUtil.create2DPoint(2,2));
        expected15.add(TestUtil.create2DPoint(1,0));
        expected15.add(TestUtil.create2DPoint(3,9));
        expectedMap.put(TestUtil.create2DPoint(-1,5).toString(), expected15);

        List<Point> expected104 = new ArrayList<Point>();
        expected104.add(TestUtil.create2DPoint(10,4));
        expected104.add(TestUtil.create2DPoint(11,10));
        expectedMap.put(TestUtil.create2DPoint(10,4).toString(), expected104);

        List<Point> expected1110 = new ArrayList<Point>();
        expected1110.add(TestUtil.create2DPoint(11,10));
        expected1110.add(TestUtil.create2DPoint(10,4));
        expectedMap.put(TestUtil.create2DPoint(11,10).toString(), expected1110);
        for(int i =0; i<this.points.size(); i++){
            Point curPoint = this.points.get(i);
            List<Point> expectedList = expectedMap.get(curPoint.toString());
            List<Point> actualList = this.operationExecutor.circleRangeQuery(curPoint, 6f, this.partitionedData, this.globalTree).collect();
            for(int j =0; j<actualList.size(); j++){
                Assert.assertEquals("Not found point in actual list: " + expectedList.get(j) , true, actualList.contains(expectedList.get(j)));
            }
            for(int j =0; j<actualList.size(); j++){
                Assert.assertEquals("Not found point in expected list: " + actualList.get(j) , true, expectedList.contains(actualList.get(j)));
            }
        }
    }

    @Test
    public void testDistanceJoin() throws Exception {
        prepareData();
        samplePoints = points.subList(0, points.size()/2);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        int parallelism = env.getParallelism();
        double sampleRate = 0.8;

        DataSet<Point> pointDS = env.fromCollection(points);
        DataSet<Point> pointDS2 = env.fromCollection(points);

        IndexBuilderResult indexResult = indexBuilder.buildIndex(pointDS, NB_DIMENSION, POINTS_PER_NODE, sampleRate, parallelism);
        DataSet<RTree> globalTree = indexResult.getGlobalRTree();


        IndexBuilderResult indexResult2 = indexBuilder.buildIndex(pointDS2, NB_DIMENSION, POINTS_PER_NODE, sampleRate, parallelism);
        DataSet<RTree> globalTree2 = indexResult2.getGlobalRTree();

        Map<String, List<Point>> expectedMap = new HashMap<String, List<Point>>();
        List<Point> expected10 = new ArrayList<Point>();
        expected10.add(TestUtil.create2DPoint(1,0));
        expected10.add(TestUtil.create2DPoint(1,2));
        expected10.add(TestUtil.create2DPoint(2,2));
        expected10.add(TestUtil.create2DPoint(-1,5));
        expectedMap.put(TestUtil.create2DPoint(1,0).toString(),expected10);

        List<Point> expected12 = new ArrayList<Point>();
        expected12.add(TestUtil.create2DPoint(1,2));
        expected12.add(TestUtil.create2DPoint(1,0));
        expected12.add(TestUtil.create2DPoint(2,2));
        expected12.add(TestUtil.create2DPoint(-1,5));
        expectedMap.put(TestUtil.create2DPoint(1,2).toString(), expected12);

        List<Point> expected22 = new ArrayList<Point>();
        expected22.add(TestUtil.create2DPoint(2,2));
        expected22.add(TestUtil.create2DPoint(1,0));
        expected22.add(TestUtil.create2DPoint(1,2));
        expected22.add(TestUtil.create2DPoint(-1,5));
        expectedMap.put(TestUtil.create2DPoint(2,2).toString(), expected22);

        List<Point> expected39 = new ArrayList<Point>();
        expected39.add(TestUtil.create2DPoint(3,9));
        expected39.add(TestUtil.create2DPoint(-1,5));
        expected39.add(TestUtil.create2DPoint(2,2));
        expectedMap.put(TestUtil.create2DPoint(3,9).toString(), expected39);

        List<Point> expected15 = new ArrayList<Point>();
        expected15.add(TestUtil.create2DPoint(-1,5));
        expected15.add(TestUtil.create2DPoint(1,2));
        expected15.add(TestUtil.create2DPoint(2,2));
        expected15.add(TestUtil.create2DPoint(1,0));
        expected15.add(TestUtil.create2DPoint(3,9));
        expectedMap.put(TestUtil.create2DPoint(-1,5).toString(), expected15);

        List<Point> expected104 = new ArrayList<Point>();
        expected104.add(TestUtil.create2DPoint(10,4));
        expected104.add(TestUtil.create2DPoint(11,10));
        expectedMap.put(TestUtil.create2DPoint(10,4).toString(), expected104);

        List<Point> expected1110 = new ArrayList<Point>();
        expected1110.add(TestUtil.create2DPoint(11,10));
        expected1110.add(TestUtil.create2DPoint(10,4));
        expectedMap.put(TestUtil.create2DPoint(11,10).toString(), expected1110);

        List<Tuple2<Point, Point>> result = this.operationExecutor.distanceJoin(6f, pointDS, globalTree, pointDS2, globalTree2).collect();
        for(int i =0; i<result.size(); i++){
            Point left = result.get(i).f0;
            Point right = result.get(i).f1;
            List<Point> expectedList = expectedMap.get(left.toString());
            Assert.assertEquals("Not found point in expected list: " + left , true, expectedList.contains(right));
        }
    }


}
