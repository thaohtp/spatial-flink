package flink.benchmark;

import flink.IndexBuilder;
import flink.datatype.Point;
import flink.util.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.hadoop.shaded.org.apache.commons.httpclient.util.DateUtil;

import java.util.Date;

/**
 * Created by JML on 5/23/17.
 */
public class IndexBenchmark {
    public static void main(String[] args) throws Exception {
        // Benchmark indexing time
        final ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input");
        Double sampleRate = params.getDouble("samplerate", 0.1);
        Integer maxNodePerEntry = params.getInt("nodeperentry", 3);
        final Integer nbDimension = params.getInt("nbdimension", 2);
        String output = params.get("output");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // make params available on web interface
        env.getConfig().setGlobalJobParameters(params);

        DataSet<Point> data = env.readTextFile(input)
                .map(new MapFunction<String, Point>() {
                    @Override
                    public Point map(String s) throws Exception {
                        String[] parts = s.split(",");
                        System.out.println(s);
                        if(nbDimension == 3){
                            return Utils.create3DPoint(Float.parseFloat(parts[0]), Float.parseFloat(parts[1]), Float.parseFloat(parts[2]));
                        }
                        else{
                            return Utils.create2DPoint(Float.parseFloat(parts[0]), Float.parseFloat(parts[1]));
                        }
                    }
                });
        data.writeAsText(output, FileSystem.WriteMode.OVERWRITE);

        Long startTime = System.currentTimeMillis();
        System.out.println("Start time: " + startTime + " - " + new Date());

        IndexBuilder indexBuilder = new IndexBuilder();
        indexBuilder.buildIndex(data, nbDimension, maxNodePerEntry, sampleRate, env.getParallelism());

        Long endTime = System.currentTimeMillis();
        System.out.println("End time: " + endTime + " - " + new Date());
        System.out.println("Total time: " +(endTime - startTime));

    }
}
