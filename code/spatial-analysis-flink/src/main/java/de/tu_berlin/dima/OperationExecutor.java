package de.tu_berlin.dima;

import de.tu_berlin.dima.datatype.MBR;
import de.tu_berlin.dima.datatype.PartitionedMBR;
import de.tu_berlin.dima.datatype.Point;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Created by JML on 6/27/17.
 */
public class OperationExecutor {

    public DataSet<Point> boxRangeQuery(final MBR box, DataSet<Point> partitionedData, DataSet<RTree> globalTree, DataSet<RTree> localTrees, int numPartitions, ExecutionEnvironment env){
        // initialize empty vector
        DataSet<PartitionedMBR> partitionedMBRS = globalTree.flatMap(new RichFlatMapFunction<RTree, PartitionedMBR>() {
            @Override
            public void flatMap(RTree rTree, Collector<PartitionedMBR> collector) throws Exception {
                List<PartitionedMBR> partitionedMBRS = rTree.search(box);
                for(int i =0; i<partitionedMBRS.size(); i++){
                    collector.collect(partitionedMBRS.get(i));
                }
            }
        });

        DataSet<Point> result = partitionedData
                .map(new RichMapFunction<Point, Tuple2<Point, Integer>>() {
                    @Override
                    public Tuple2<Point, Integer> map(Point point) throws Exception {
                        return new Tuple2<Point, Integer>(point, getRuntimeContext().getIndexOfThisSubtask());
                    }
                }).joinWithTiny(partitionedMBRS)
                .where(1)
                .equalTo(new KeySelector<PartitionedMBR, Integer>() {
                    @Override
                    public Integer getKey(PartitionedMBR partitionedMBR) throws Exception {
                        return partitionedMBR.getPartitionNumber();
                    }
                })
        .flatMap(new RichFlatMapFunction<Tuple2<Tuple2<Point,Integer>,PartitionedMBR>, Point>() {
            @Override
            public void flatMap(Tuple2<Tuple2<Point, Integer>, PartitionedMBR> tuple, Collector<Point> collector) throws Exception {
                if(box.contains(tuple.f0.f0)){
                    collector.collect(tuple.f0.f0);
                }
            }
        });

        return result;
    }

}
