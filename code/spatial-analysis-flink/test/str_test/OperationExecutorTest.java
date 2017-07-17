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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Suite;

import java.util.ArrayList;
import java.util.List;

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

    }
}
