package de.tu_berlin.dima;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

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

        DataSet<Tuple2<Integer, String>> data = env.fromElements(
                new Tuple2<Integer, String>(1, "a"), new Tuple2<Integer, String>(1000, "b"),
                new Tuple2<Integer, String>(2, "k"), new Tuple2<Integer, String>(1020, "c"));

        DataSet<Tuple2<Integer, String>> partitionedData =
                data.partitionCustom(new MyPartitioner(), 0);

        partitionedData.print();
        partitionedData.writeAsCsv("/jml/data/test/spatial_analysis_flink/dummy_test", FileSystem.WriteMode.OVERWRITE);
        env.execute("Dummy test in Flink");
    }
}