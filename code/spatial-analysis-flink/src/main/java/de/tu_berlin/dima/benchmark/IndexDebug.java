package de.tu_berlin.dima.benchmark;

import de.tu_berlin.dima.datatype.MBR;
import de.tu_berlin.dima.datatype.Point;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.*;

/**
 * Created by JML on 7/10/17.
 */
public class IndexDebug {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String input = "/jml/data/d/1 CLASS STUDY/1 Lecture/1 IT4BI Second/0 Thesis/3 Lab/benchmark_result/error_log/input/";
        String output = "/jml/data/d/1 CLASS STUDY/1 Lecture/1 IT4BI Second/0 Thesis/3 Lab/benchmark_result/error_log/output";
        DataSet<MBR> mbrs = env.readTextFile(input)
                .flatMap(new FlatMapFunction<String, MBR>() {
                    @Override
                    public void flatMap(String s, Collector<MBR> collector) throws Exception {
                        String[] splits = s.split(",");
                        Point min = IndexDebug.create2DPoint(Float.parseFloat(splits[0]), Float.parseFloat(splits[1]));
                        Point max = IndexDebug.create2DPoint(Float.parseFloat(splits[2]), Float.parseFloat(splits[3]));
                        MBR mbr = new MBR(min, max);
                        collector.collect(mbr);
                    }
                });
        List<MBR> mbrList = mbrs.collect();
        Map<String, MBR> mbr = new HashMap<String, MBR>();
        for(int i =0; i<mbrList.size(); i++){
//            System.out.println(mbrList.get(i));
        }
        System.out.println(mbrList.size());

        Collections.sort(mbrList, new Comparator<MBR>() {
            @Override
                        public int compare(MBR o1, MBR o2) {
//                            System.out.println(o1);
                            return o1.compare(o2, 0);
                        }
        });

//        mbrs.mapPartition(new RichMapPartitionFunction<MBR, MBR>() {
//            @Override
//            public void mapPartition(Iterable<MBR> iterable, Collector<MBR> collector) throws Exception {
////                System.out.println("Partition: " + getRuntimeContext().getIndexOfThisSubtask());
//                List<MBR> mbrList = new ArrayList<MBR>();
//                Iterator<MBR> mbrIter = iterable.iterator();
//                while(mbrIter.hasNext()){
//                    mbrList.add(mbrIter.next());
//                }
//
//
//                try{
//                    Collections.sort(mbrList, new Comparator<MBR>() {
//                        @Override
//                        public int compare(MBR o1, MBR o2) {
//                            return o1.compare(o2, 0);
//                        }
//                    });
//                }
//                catch (Exception ex){
//                    System.out.println(ex);
//                    System.out.println(getRuntimeContext().getIndexOfThisSubtask());
//                    for(int i =0; i<mbrList.size(); i++){
//                        collector.collect(mbrList.get(i));
//                    }
//                }
//            }
//        }).print();
//                .rebalance()
//                .writeAsFormattedText(output, FileSystem.WriteMode.OVERWRITE, new TextOutputFormat.TextFormatter<MBR>() {
//                    @Override
//                    public String format(MBR value) {
//                        Point minPoint = value.getMinPoint();
//                        Point maxPoint = value.getMaxPoint();
//                        String result = minPoint.getDimension(0) + "," + minPoint.getDimension(1);
//                        result = result + "," + maxPoint.getDimension(0) + "," + maxPoint.getDimension(1);
//                        return result;
//                    }
//                });
//        env.execute();
    }

    public static Point create2DPoint(float x, float y){
        List<Float> floatList = new ArrayList<Float>(2);
        floatList.add(0, x);
        floatList.add(1, y);
        return new Point(floatList);
    }
}
