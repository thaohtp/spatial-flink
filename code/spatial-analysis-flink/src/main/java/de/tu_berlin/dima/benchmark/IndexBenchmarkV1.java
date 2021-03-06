package de.tu_berlin.dima.benchmark;

import de.tu_berlin.dima.IndexBuilder;
import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.datatype.Point;
import de.tu_berlin.dima.test.IndexBuilderResult;
import de.tu_berlin.dima.util.Utils;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

/**
 * Created by JML on 5/23/17.
 */
public class IndexBenchmarkV1 {
    public static void main(String[] args) throws Exception {
        Logger LOG = LoggerFactory.getLogger(IndexBenchmark.class);

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
        List<Long> localTreeSize = result.getLocalRTree().map(new MapFunction<RTree, Long>() {
            @Override
            public Long map(RTree rTree) throws Exception {
                return rTree.getNumBytes();
            }
        }).collect();
        RTree globalTree = result.getGlobalRTree().collect().get(0);

        System.out.println("\n---------------- Statistics -------------");
        System.out.println("Start building index: " + startTime + " - " + new Date());
        System.out.println("End building index: " + endTime + " - " + new Date());
        System.out.println("Total time of building index: " +(endTime - startTime) + " ms");
        System.out.println("---------------- End statistics -------------");

        System.out.println("\n---------------- Local trees -------------");
        long totalSize = 0;
        for(int i =0; i< localTreeSize.size(); i++){
            Long size = localTreeSize.get(i);
            System.out.println("Tree size " + size);
            totalSize += size;
        }
        System.out.println("Local trees (total size): " + totalSize);
        System.out.println("---------------- End local trees ---------");

        System.out.println("\n---------------- Global tree -------------");
        System.out.println(globalTree.getRootNode().getSize() + "," + globalTree.getRootNode().getMbr());
        System.out.println(globalTree.toString());
        System.out.println("Memory: " + globalTree.getRootNode().getNumBytes());
        System.out.println("---------------- End global tree ---------");

        // benchmark index storage over head
    }

}
