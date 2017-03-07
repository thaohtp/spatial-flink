package flink;

import flink.datatype.Point;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by JML on 2/4/17.
 */
public class PartitionerExample {
    public static class MyPartitioner implements Partitioner<Integer> {
        @Override
        public int partition(Integer key, int numPartitions) {
            return key % numPartitions;
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
                        return new Point(tuple.f0, tuple.f1);
                    }
                });
//                data.partitionCustom(new MyPartitioner(), 0);
        
        partitionedData.print();

        partitionedData.mapPartition(new MapPartitionFunction<Point, Object>() {
            @Override
            public void mapPartition(Iterable<Point> iterable, Collector<Object> collector) throws Exception {
                List<Point> subList = new ArrayList<Point>();
                Iterator<Point> pointIter = iterable.iterator();
                while(pointIter.hasNext()){
                    subList.add(pointIter.next());
                }

                STRPacking strPacking = new STRPacking(4, 2, subList);
                strPacking.sort();
                for(int i =0; i<strPacking.getSize(); i++){
                    collector.collect(strPacking.getPoint(i));
                }

            }
        })
                .print();
//        partitionedData.writeAsCsv("/jml/data/test/spatial_analysis_flink/dummy_test", FileSystem.WriteMode.OVERWRITE);
        env.execute("Dummy test in Flink");
    }
}