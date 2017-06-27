package de.tu_berlin.dima;

import de.tu_berlin.dima.datatype.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

/**
 * Created by JML on 6/27/17.
 */
public class OperationExecutor {
    public static void main(String[] args){

    }

    public DataSet<Point> boxRangeQuery(final MBR box, DataSet<Point> partitionedData, RTree globalTree, DataSet<RTree> localTrees, int numPartitions){
        // initialize empty vector
        final boolean[] flags = new boolean[numPartitions];
        List<PartitionedMBR> partitionedMBRS = globalTree.search(box);
        for (PartitionedMBR partitionedMBR: partitionedMBRS) {
            int index = partitionedMBR.getPartitionNumber();
            flags[index] = true;
        }

        // intersect box and MBR in global RTree
        DataSet<Point> result = partitionedData.filter(new RichFilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                int partitionIndex = getRuntimeContext().getIndexOfThisSubtask();
                return flags[partitionIndex];
            }
        })
        .filter(new RichFilterFunction<Point>() {
            @Override
            public boolean filter(Point point) throws Exception {
                return box.contains(point);
            }
        });
        return result;
    }

}
