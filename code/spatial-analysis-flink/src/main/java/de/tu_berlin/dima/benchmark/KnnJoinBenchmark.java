package de.tu_berlin.dima.benchmark;

import de.tu_berlin.dima.IndexBuilder;
import de.tu_berlin.dima.OperationExecutor;
import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.datatype.Point;
import de.tu_berlin.dima.test.IndexBuilderResult;
import de.tu_berlin.dima.util.RTreeBinaryInputFormat;
import de.tu_berlin.dima.util.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;

/**
 * Created by JML on 7/27/17.
 */
public class KnnJoinBenchmark {
    public static void main(String[] args) throws Exception {
        Logger LOG = LoggerFactory.getLogger(CircleRangeQueryBenchmark.class);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);

        String input1 = params.get("input1");
        String globalRTreeInput1 = params.get("input1") + "/globaltree";
        String dataInput1 = params.get("input1") + "/data";

        String input2 = params.get("input2");
        String globalRTreeInput2 = params.get("input2") + "/globaltree";
        String dataInput2 = params.get("input2") + "/data";

        String output = params.get("output");
        Integer nbDimension = params.getInt("nbDimension", 2);
        Integer maxNodePerEntry = params.getInt("nodeperentry", 64);
        Float sampleRate = params.getFloat("samplerate", 0.1f);

        Integer k = params.getInt("k");

        // read data and rtree
        DataSet<Point> partitionedData1 = env.readTextFile(dataInput1)
                .map(new MapFunction<String, Point>() {
                    @Override
                    public Point map(String s) throws Exception {
                        String[] parts = s.split(",");
                        return Utils.create2DPoint(Float.parseFloat(parts[0]), Float.parseFloat(parts[1]));
                    }
                });

        IndexBuilder indexBuilder = new IndexBuilder();
        IndexBuilderResult indexBuilderResult1 = indexBuilder.buildIndexWithoutPartition(partitionedData1, nbDimension, maxNodePerEntry);
        DataSet<RTree> globalTrees1 = indexBuilderResult1.getGlobalRTree();

        DataSet<Point> partitionedData2 = env.readTextFile(dataInput2)
                .map(new MapFunction<String, Point>() {
                    @Override
                    public Point map(String s) throws Exception {
                        String[] parts = s.split(",");
                        return Utils.create2DPoint(Float.parseFloat(parts[0]), Float.parseFloat(parts[1]));
                    }
                });

        OperationExecutor executor = new OperationExecutor(nbDimension, maxNodePerEntry, env.getParallelism(), sampleRate);
        DataSet<Tuple2<Point, Point>> result = executor.kNNJoin(k, partitionedData1, globalTrees1, partitionedData2);
        result.writeAsCsv(output + "/data",  "\n", " - ", FileSystem.WriteMode.OVERWRITE);
        env.execute();
    }
}
