package str_test;

import de.tu_berlin.dima.IndexBuilder;
import de.tu_berlin.dima.OperationExecutor;
import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.datatype.MBR;
import de.tu_berlin.dima.datatype.Point;
import de.tu_berlin.dima.test.IndexBuilderResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
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

    public static void main(String[] args) throws Exception {
        OperationExecutorTest test = new OperationExecutorTest();
        test.testBoxRangeQuery();
    }


    // What to test
    // 1. Test STRPartitioner
    // 2. Test updating boundaries when we do the partitioning
    // 3. Test local RTree
    // 4. Test the global RTree

    // TODO: test with sample size = 1

    @Test
    public void testBoxRangeQuery() throws Exception {
        prepareData();
        samplePoints = points.subList(0, points.size()/2);

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        int parallelism = env.getParallelism();
        double sampleRate = 0.8;

        DataSet<Point> pointDS = env.fromCollection(points);
        DataSet<Point> samplePointDS = env.fromCollection(samplePoints);

        IndexBuilderResult indexResult = indexBuilder.buildIndexTestVersion(pointDS, pointDS, NB_DIMENSION, POINTS_PER_NODE, sampleRate, parallelism);

        System.out.println("Global tree: " + indexResult.getGlobalRTree().toString());
        System.out.println("Local tree: ");
        indexResult.getLocalRTree().print();
        System.out.println(indexResult.getLocalRTree().count());

        DataSet<Point> partitionedData = indexResult.getData();
        DataSet<RTree> globalTree = indexResult.getGlobalRTree();
        DataSet<RTree> localTrees = indexResult.getLocalRTree();

        Point p1 = TestUtil.create2DPoint(-1,-1);
        Point p2 = TestUtil.create2DPoint(-1,5);
        MBR mbr = new MBR(2);
        mbr.addPoint(p1);
        mbr.addPoint(p2);

        DataSet<Point> result = this.operationExecutor.boxRangeQuery(mbr, partitionedData, globalTree, localTrees, 4, env);
        result.print();
    }
}
