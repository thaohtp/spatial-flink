package de.tu_berlin.dima.benchmark;

import de.tu_berlin.dima.IndexBuilder;
import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.datatype.Point;
import de.tu_berlin.dima.test.IndexBuilderResult;
import de.tu_berlin.dima.util.RTreeBinayOutputFormat;
import de.tu_berlin.dima.util.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by JML on 5/23/17.
 */
public class DataPreparation {
    public static void main(String[] args) throws Exception {
        Logger LOG = LoggerFactory.getLogger(DataPreparation.class);

        // Benchmark indexing time
        final ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input");
        final Double indexSampleRate = params.getDouble("indexsamplerate", 0.1);
        final Integer maxNodePerEntry = params.getInt("nodeperentry", 3);
        final Integer nbDimension = params.getInt("nbdimension", 2);
        String output = params.get("output");
        String localRTreeOutput = params.get("output") + "/localtree";
        String globalRTreeOutput = params.get("output") + "/globaltree";
        String dataOutput = params.get("output") + "/data";

        Double dataSampleRate = params.getDouble("datasamplerate", 0.1);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
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
        DataSet<Point> sampleData = DataSetUtils.sample(data, false, dataSampleRate);

        IndexBuilder indexBuilder = new IndexBuilder();
        IndexBuilderResult result = indexBuilder.buildIndex(sampleData, nbDimension, maxNodePerEntry, indexSampleRate, env.getParallelism());

        // local rtree size
        final DataSet<RTree> localTrees = result.getLocalRTree();
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

        localTrees.write( new RTreeBinayOutputFormat(), localRTreeOutput, FileSystem.WriteMode.OVERWRITE);
        env.execute();

        result.getGlobalRTree().write(new RTreeBinayOutputFormat(), globalRTreeOutput, FileSystem.WriteMode.OVERWRITE);
        env.execute();

    }
}
