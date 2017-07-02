package de.tu_berlin.dima.benchmark;

import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.IndexBuilder;
import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.datatype.MBR;
import de.tu_berlin.dima.datatype.Point;
import de.tu_berlin.dima.datatype.RTreeNode;
import de.tu_berlin.dima.serializer.PointSerializer;
import de.tu_berlin.dima.serializer.RTreeSerializer;
import de.tu_berlin.dima.test.IndexBuilderResult;
import de.tu_berlin.dima.util.Utils;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.commons.math3.stat.inference.TestUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.instrument.Instrumentation;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created by JML on 5/23/17.
 */
public class IndexBenchmark {
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
        Utils.registerTypeWithKryoSerializer(env);
        Utils.registerCustomSerializer(env);

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
        RTree globalTree = result.getGlobalRTree().collect().get(0);
        DataSet<Point> partitionedData = result.getData();

        partitionedData.writeAsFormattedText(dataOutput, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<Point>() {
            @Override
            public String format(Point point) {
                return point.toString();
            }
        });

        System.out.println("\n---------------- Statistics -------------");
        System.out.println("Start building index: " + startTime + " - " + new Date());
        System.out.println("End building index: " + endTime + " - " + new Date());
        System.out.println("Total time of building index: " +(endTime - startTime) + " ms");
        System.out.println("---------------- End statistics -------------");

        System.out.println("\n---------------- Local trees -------------");
        localTrees.map(new RichMapFunction<RTree, String>() {

            private Counter counter;

            @Override
            public void open(Configuration config) {
                this.counter = getRuntimeContext()
                        .getMetricGroup()
                        .counter("myCounter");
            }


            @Override
            public String map(RTree rTree) throws Exception {
                this.counter.inc();
                System.out.println("Metrics: " + getRuntimeContext().getMetricGroup().getMetricIdentifier("Memory.Heap.Used"));
                return "Local tree: ," + rTree.getRootNode().getSize() + "," + rTree.getRootNode().getMbr();
            }
        }).print();

        System.out.println("---------------- End local trees ---------");

        System.out.println("\n---------------- Global tree -------------");
        System.out.println(globalTree.getRootNode().getSize() + "," + globalTree.getRootNode().getMbr());
        System.out.println(globalTree.toString());
        System.out.println("---------------- End global tree ---------");

        // benchmark index storage over head
    }
}
