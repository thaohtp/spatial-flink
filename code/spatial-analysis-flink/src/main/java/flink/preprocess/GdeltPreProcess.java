package flink.preprocess;

import flink.benchmark.IndexBenchmark;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by JML on 5/29/17.
 */
public class GdeltPreProcess {
    public static void main(String[] args) throws Exception {
        Logger LOG = LoggerFactory.getLogger(GdeltPreProcess.class);

        // Benchmark indexing time
        final ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input");
        String output = params.get("output");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // make params available on web interface
        env.getConfig().setGlobalJobParameters(params);
        DataSet<String> data = env.readTextFile(input)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String s) throws Exception {
                        String[] parts = s.split("\t");;
                        // get col 53, 54
                        if(parts[53].isEmpty() || parts[54].isEmpty()){
                            return "";
                        }
                        return parts[53] + "," + parts[54];
                    }
                })
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String s) throws Exception {
                        return !s.isEmpty();
                    }
                });
        data.writeAsFormattedText(output, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<String>() {
            @Override
            public String format(String s) {
                return s;
            }
        });
        env.execute("Gdelt preprocess");
    }
}
