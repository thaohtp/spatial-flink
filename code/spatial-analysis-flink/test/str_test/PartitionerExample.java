package str_test;

import flink.RTree;
import flink.STRPacking;
import flink.STRPartitioner;
import flink.datatype.PartitionedMBR;
import flink.datatype.Point;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by JML on 2/4/17.
 */
public class PartitionerExample {
    private static int MAX_POINT_PER_NODE = 4;

    public static class MyPartitioner implements Partitioner<Integer> {

        public MyPartitioner(){

        }

        public MyPartitioner(RTree rTree){

        }

        @Override
        public int partition(Integer key, int numPartitions) {

            return Math.abs(key) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, Integer>> data = env.fromElements(
                new Tuple2<Integer, Integer>(1, 1), new Tuple2<Integer, Integer>(1, 2),
                new Tuple2<Integer, Integer>(2, 3), new Tuple2<Integer, Integer>(1020, 3),
                new Tuple2<Integer, Integer>(45, 3), new Tuple2<Integer, Integer>(40, 3),
                new Tuple2<Integer, Integer>(-1, 5), new Tuple2<Integer, Integer>(0, -1));

        DataSet<Point> partitionedData =
                data.map(new MapFunction<Tuple2<Integer, Integer>, Point>() {
                    @Override
                    public Point map(Tuple2<Integer, Integer> tuple) throws Exception {
                        List<Float> floatList = new ArrayList<Float>(2);
                        floatList.add(0, (float) tuple.f0);
                        floatList.add(1, (float) tuple.f1);
                        return new Point(floatList);
                    }
                })
                .partitionCustom(new MyPartitioner(), new KeySelector<Point, Integer>() {
                    @Override
                    public Integer getKey(Point point) throws Exception {
                        System.out.println(point.getDimension(0));
                        return (int) point.getDimension(0);
                    }
                });

//        partitionedData.print();
        partitionedData.mapPartition(new MapPartitionFunction<Point, Object>() {
            @Override
            public void mapPartition(Iterable<Point> iterable, Collector<Object> collector) throws Exception {

            }
        });

        DataSet<RTree> rtrees = partitionedData.mapPartition(new MapPartitionFunction<Point, RTree>() {
            @Override
            public void mapPartition(Iterable<Point> iterable, Collector<RTree> collector) throws Exception {
                List<Point> pointList = new ArrayList<Point>();
                Iterator<Point> pointIter = iterable.iterator();
                while (pointIter.hasNext()){
                    pointList.add(pointIter.next());
                }

                STRPacking strPacking = new STRPacking(3,2);
                RTree rtree = strPacking.createRTree(pointList);
                collector.collect(rtree);
            }
        });
        rtrees.print();

        // Global RTree should contain MBR and partition number

        DataSet<RTree> globalRTree = rtrees.reduceGroup(new GroupReduceFunction<RTree, RTree>() {
            @Override
            public void reduce(Iterable<RTree> iterable, Collector<RTree> collector) throws Exception {
                Iterator<RTree> rtreeIter = iterable.iterator();
                int i =0;
                List<PartitionedMBR> partitionedMBRList = new ArrayList<PartitionedMBR>();
                while(rtreeIter.hasNext()){
                    i++;
                    RTree rtree = rtreeIter.next();
                    PartitionedMBR point = new PartitionedMBR(rtree.getRootNode().getMbr(), i);
                    partitionedMBRList.add(point);
                }

                STRPacking strPacking = new STRPacking(3,2);
                RTree globalTree = strPacking.createGlobalRTree(partitionedMBRList);
                collector.collect(globalTree);
            }
        });
        globalRTree.print();

        // Test partitioner
        RTree rTree = globalRTree.collect().get(0);
        System.out.println("Search result " + rTree.getRootNode().getMbr().contains(TestUtil.create2DPoint(2,3)));

        partitionedData.partitionCustom(new STRPartitioner(rTree), new KeySelector<Point, Point>() {
            @Override
            public Point getKey(Point point) throws Exception {
                return point;
            }
        }).print();

//        partitionedData.partitionCustom(new MyPartitioner(rTree), new KeySelector<Point, Integer>() {
//            @Override
//            public Integer getKey(Point point) throws Exception {
//                return (int) point.getDimension(0);
//            }
//        });

//        partitionedData.partitionCustom(new STRPartitioner(rTree), String.valueOf(new KeySelector<Point, Point>() {
//            @Override
//            public Point getKey(Point point) throws Exception {
//                return point;
//            }
//        }));

    }
}