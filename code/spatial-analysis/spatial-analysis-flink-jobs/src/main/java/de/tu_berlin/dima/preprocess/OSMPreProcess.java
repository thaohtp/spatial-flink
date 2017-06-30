package de.tu_berlin.dima.preprocess;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

/**
 * Created by JML on 6/22/17.
 */
public class OSMPreProcess {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input");
        String output = params.get("output");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // make params available on web interface
        env.getConfig().setGlobalJobParameters(params);
        DataSet<String> data = env.readTextFile(input)
                .flatMap(new FlatMapFunction<String, Tuple2<Boolean, String>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<Boolean, String>> collector) throws Exception {
                        String tagName = "<changeset";
                        if(s.contains(tagName)){
                            String[] splits = s.split("\"");
                            String min_longitude = "";
                            String min_latitude = "";
                            String max_longitude = "";
                            String max_latitude = "";
                            for(int i =1; i < splits.length; i++){
                                if(splits[i].contains("min_lon")){
                                    min_longitude = splits[i+1];
                                    i++;
                                }
                                else{
                                    if(splits[i].contains("min_lat")){
                                        min_latitude = splits[i+1];
                                        collector.collect(new Tuple2<Boolean, String>(true, min_longitude + "," +min_latitude));
                                    }
                                    else{
                                        if(splits[i].contains("max_lon")){
                                            max_longitude = splits[i+1];
                                            i++;
                                        }
                                        else{
                                            if(splits[i].contains("max_lat")){
                                                max_latitude = splits[i+1];
                                                collector.collect(new Tuple2<Boolean, String>(true, max_longitude + "," +max_latitude));
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        collector.collect(new Tuple2<Boolean, String>(false, "  "));
                    }
                })
                .filter(new FilterFunction<Tuple2<Boolean, String>>() {
                    @Override
                    public boolean filter(Tuple2<Boolean, String> tuple2) throws Exception {
                        return tuple2.f0;
                    }
                })
                .map(new MapFunction<Tuple2<Boolean, String>, String>() {
                    @Override
                    public String map(Tuple2<Boolean, String> tuple) throws Exception {
                        return tuple.f1;
                    }
                });
//                .print();
//        data.print();
        data.writeAsFormattedText(output, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<String>() {
            @Override
            public String format(String s) {
                return s;
            }
        });
        env.execute("Open Street Map preprocess");
    }
}
