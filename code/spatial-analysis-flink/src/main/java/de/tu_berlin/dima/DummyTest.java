package de.tu_berlin.dima;

import de.tu_berlin.dima.benchmark.IndexBenchmarkV1;
import de.tu_berlin.dima.datatype.Point;
import de.tu_berlin.dima.serializer.RTreeSerializer;
import de.tu_berlin.dima.test.IndexBuilderResult;
import de.tu_berlin.dima.util.RTreeBinaryInputFormat;
import de.tu_berlin.dima.util.RTreeBinayOutputFormat;
import de.tu_berlin.dima.util.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.io.BinaryInputFormat;
import org.apache.flink.api.common.io.BinaryOutputFormat;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.io.TypeSerializerInputFormat;
import org.apache.flink.api.java.io.TypeSerializerOutputFormat;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

/**
 * Created by JML on 5/23/17.
 */
public class DummyTest {
    public static void main(String[] args) throws Exception {
        Logger LOG = LoggerFactory.getLogger(IndexBenchmarkV1.class);

        // Benchmark indexing time
        final ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input");
        final Double sampleRate = params.getDouble("samplerate", 0.1);
        final Integer maxNodePerEntry = params.getInt("nodeperentry", 3);
        final Integer nbDimension = params.getInt("nbdimension", 2);
        String output = params.get("output");
        String localRTreeOutput = params.get("output") + "/localtree";
        String globalRTreeOutput = params.get("output") + "/globaltree";
        String dataOutput = params.get("output") + "/data";

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // make params available on web interface
        env.getConfig().setGlobalJobParameters(params);
//        Utils.registerTypeWithKryoSerializer(env);
//        Utils.registerCustomSerializer(env);
        DataSet<Point> data = env.readTextFile(input)
                .map(new MapFunction<String, Point>() {
                    @Override
                    public Point map(String s) throws Exception {
                        String[] parts = s.split(",");
                        if(nbDimension == 3){
                            return Utils.create3DPoint(Float.parseFloat(parts[0]), Float.parseFloat(parts[1]), Float.parseFloat(parts[2]));
                        }
                        else{
                            return Utils.create2DPoint(Float.parseFloat(parts[0]), Float.parseFloat(parts[1]));
                        }
                    }
                });

        Long startTime = System.currentTimeMillis();
        IndexBuilder indexBuilder = new IndexBuilder();
        IndexBuilderResult result = indexBuilder.buildIndex(data, nbDimension, maxNodePerEntry, sampleRate, env.getParallelism());
        Long endTime = System.currentTimeMillis();


        // local rtree size
        final DataSet<RTree> localTrees = result.getLocalRTree();
//        RTree globalTree = result.getGlobalRTree().collect().get(0);
        DataSet<Point> partitionedData = result.getData();
        partitionedData.writeAsFormattedText(dataOutput, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<Point>() {
                    @Override
                    public String format(Point value) {
                        StringBuilder builder = new StringBuilder("");
                        for(int i =0; i<value.getNbDimension(); i++){
                            builder.append(value.getDimension(i) + ",");
                        }
                        builder.deleteCharAt(builder.length()-1);
                        return builder.toString();
                    }
                });
        env.execute();

//        partitionedData.writeAsFormattedText(dataOutput, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<Point>() {
//            @Override
//            public String format(Point point) {
//                return point.toString();
//            }
//        });

//        localTrees.print();



        localTrees.write( new RTreeBinayOutputFormat(), localRTreeOutput, FileSystem.WriteMode.OVERWRITE);
        env.execute();

//        DataSet<RTree> inputRTrees = env.readFile( new RTreeBinaryInputFormat(), localRTreeOutput);
//        inputRTrees.print();


        result.getGlobalRTree().write(new RTreeBinayOutputFormat(), globalRTreeOutput, FileSystem.WriteMode.OVERWRITE);
        env.execute();

    }
}
