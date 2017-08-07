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
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 7/27/17.
 */
public class CircleRangeQueryBenchmarkPrint {
    public static void main(String[] args) throws Exception {
        Logger LOG = LoggerFactory.getLogger(CircleRangeQueryBenchmarkPrint.class);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // Benchmark indexing time
        final ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input");
        String queryPointInput = params.get("queryinput");
        String output = params.get("output");
        String localRTreeInput = params.get("input") + "/localtree";
        String globalRTreeInput = params.get("input") + "/globaltree";
        String dataInput = params.get("input") + "/data";
        Integer nbDimension = params.getInt("nbDimension", 2);
        Integer maxNodePerEntry = params.getInt("nodeperentry", 64);
        Float sampleRate = params.getFloat("samplerate", 0.1f);

        Integer nbJobs = params.getInt("nbJobs", 10);
        Float range = params.getFloat("radius");

        // read data and rtree

        List<Point> queryPoints = new ArrayList<Point>();
        BufferedReader breader = new BufferedReader(new FileReader(queryPointInput));
        String line = "";
        while ((line = breader.readLine()) != null){
            String[] parts = line.split(",");
            queryPoints.add(Utils.create2DPoint(Float.parseFloat(parts[0]), Float.parseFloat(parts[1])));
        }
        breader.close();

        DataSet<Point> partitionedData = env.readTextFile(dataInput)
                .map(new MapFunction<String, Point>() {
                    @Override
                    public Point map(String s) throws Exception {
                        String[] parts = s.split(",");
                        return Utils.create2DPoint(Float.parseFloat(parts[0]), Float.parseFloat(parts[1]));
                    }
                });
        IndexBuilder indexBuilder = new IndexBuilder();
        IndexBuilderResult indexBuilderResult = indexBuilder.buildIndexWithoutPartition(partitionedData, nbDimension, maxNodePerEntry);
        DataSet<RTree> globalTrees = indexBuilderResult.getGlobalRTree();
        List<Long> running_time = new ArrayList<Long>();

        OperationExecutor executor = new OperationExecutor(nbDimension, maxNodePerEntry, env.getParallelism(), sampleRate);
        Long startTime = System.currentTimeMillis();
        Long prevTime = System.currentTimeMillis();
        for(int i =0; i< 3; i++){
            DataSet<Point> result = executor.circleRangeQuery(queryPoints.get(0), range, partitionedData, globalTrees);
            System.out.println("Result of point " + i + " - " + queryPoints.get(0));
//            result.writeAsFormattedText(output + "/result_data" + i, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<Point>() {
//                @Override
//                public String format(Point value) {
//                    return value.toString();
//                }
//            });
            result.printOnTaskManager("");
//            result.print();
//            env.execute();
            Long now = System.currentTimeMillis();
            System.out.println("Running time: " + (now - prevTime) + " ms");
            running_time.add(now - prevTime);
            prevTime = now;
        }
        env.getExecutionPlan();
        env.execute();
        Long endTime = System.currentTimeMillis();
        System.out.println("Total running time: " + (endTime - startTime) + " ms");
        running_time.add(endTime-startTime);

        BufferedWriter bwriter = new BufferedWriter(new FileWriter(output + "/running_time.csv"));
        for(int i =0; i < running_time.size(); i++){
            bwriter.write(running_time.get(i).toString());
            bwriter.write("\n");
        }
        bwriter.flush();
        bwriter.close();
    }
}
