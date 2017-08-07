package de.tu_berlin.dima.benchmark;

import de.tu_berlin.dima.OperationExecutor;
import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.datatype.Point;
import de.tu_berlin.dima.util.RTreeBinaryInputFormat;
import de.tu_berlin.dima.util.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 7/27/17.
 */
public class KnnQueryBenchmark {
    public static void main(String[] args) throws Exception {
        Logger LOG = LoggerFactory.getLogger(CircleRangeQueryBenchmark.class);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input");
        String queryPointInput = params.get("queryinput");
        String output = params.get("output");
        String dataInput = params.get("input") + "/data";
        Integer nbDimension = params.getInt("nbDimension", 2);
        Integer maxNodePerEntry = params.getInt("nodeperentry", 64);
        Float sampleRate = params.getFloat("samplerate", 0.1f);

        Integer k = params.getInt("k");

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

        List<Long> running_time = new ArrayList<Long>();
        OperationExecutor executor = new OperationExecutor(nbDimension, maxNodePerEntry, env.getParallelism(), sampleRate);
        Long startTime = System.currentTimeMillis();
        Long prevTime = System.currentTimeMillis();
        for(int i =0; i<queryPoints.size(); i++){
            DataSet<Point> result = executor.kNNQuery(queryPoints.get(i), k, partitionedData);
            System.out.println("Result of point " + i + " - " + queryPoints.get(i));
//            env.getExecutionPlan();
//            result.print();

            result.writeAsFormattedText(output + "/result_data" + i, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<Point>() {
                @Override
                public String format(Point value) {
                    return value.toString();
                }
            });
            env.getExecutionPlan();
            env.execute();

            Long now = System.currentTimeMillis();
            System.out.println("Running time: " + (now - prevTime) + " ms");
            running_time.add(now - prevTime);
            prevTime = now;
        }
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
