package de.tu_berlin.dima;

import de.tu_berlin.dima.datatype.MBR;
import de.tu_berlin.dima.datatype.PartitionedMBR;
import de.tu_berlin.dima.datatype.Point;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Created by JML on 6/27/17.
 */
public class OperationExecutor {

    public DataSet<Point> boxRangeQuery(final MBR box, DataSet<Point> partitionedData, DataSet<RTree> globalTree) {
        // initialize empty vector
        DataSet<Integer> partitionFlags = globalTree.flatMap(new RichFlatMapFunction<RTree, PartitionedMBR>() {
            @Override
            public void flatMap(RTree rTree, Collector<PartitionedMBR> collector) throws Exception {
                List<PartitionedMBR> partitionedMBRS = rTree.search(box);
                for (int i = 0; i < partitionedMBRS.size(); i++) {
                    collector.collect(partitionedMBRS.get(i));
                }
            }
        })
                .map(new MapFunction<PartitionedMBR, Integer>() {
                    @Override
                    public Integer map(PartitionedMBR partitionedMBR) throws Exception {
                        return partitionedMBR.getPartitionNumber();
                    }
                });

        DataSet<Point> result = partitionedData
                .mapPartition(new RichMapPartitionFunction<Point, Point>() {
                    @Override
                    public void mapPartition(Iterable<Point> iterable, Collector<Point> collector) throws Exception {
                        List<Integer> partitionFlags = getRuntimeContext().getBroadcastVariable("partitionFlags");
                        if (partitionFlags.contains(getRuntimeContext().getIndexOfThisSubtask())) {
                            Iterator<Point> pointIter = iterable.iterator();
                            while (pointIter.hasNext()) {
                                collector.collect(pointIter.next());
                            }
                        }

                    }
                }).withBroadcastSet(partitionFlags, "partitionFlags")
                .flatMap(new FlatMapFunction<Point, Point>() {
                    @Override
                    public void flatMap(Point point, Collector<Point> collector) throws Exception {
                        if(box.contains(point)){
                            collector.collect(point);
                        }
                    }
                });
        return result;
    }

    public DataSet<Point> kNNQuery(final Point queryPoint, final Integer k, DataSet<Point> partitionedData){
        DataSet<Point> result = partitionedData.map(new RichMapFunction<Point, Tuple2<Point, Float>>() {
            @Override
            public Tuple2<Point, Float> map(Point point) throws Exception {
                Float distance = point.calcDistance(queryPoint);
//                System.out.println("Distance " + point + ": " + distance);
                return new Tuple2<Point, Float>(point, distance);
            }
        })
                .sortPartition(1, Order.ASCENDING)
                .mapPartition(new RichMapPartitionFunction<Tuple2<Point,Float>, Tuple2<Point, Float>>() {
                    @Override
                    public void mapPartition(Iterable<Tuple2<Point, Float>> iterable, Collector<Tuple2<Point, Float>> collector) throws Exception {
                        int count = 0;
                        Iterator<Tuple2<Point, Float>> iter = iterable.iterator();
                        String temp = "SortPartition " + getRuntimeContext().getIndexOfThisSubtask() + ": ";
                        while(iter.hasNext() && count <k){
                            Tuple2<Point, Float> tuple = iter.next();
                            temp += tuple.f0.toString() + " - " + tuple.f1 + " ";
                            collector.collect(tuple);
                            count++;
                        }
                        System.out.println(temp);
                    }
                })
                .reduceGroup(new RichGroupReduceFunction<Tuple2<Point,Float>, Point>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Point, Float>> iterable, Collector<Point> collector) throws Exception {
                        Iterator<Tuple2<Point, Float>> iter = iterable.iterator();
                        List<Tuple2<Point, Float>> tupleList = new ArrayList<Tuple2<Point, Float>>();
                        int count = 0;

                        while(iter.hasNext()){
                            tupleList.add(iter.next());
                        }
                        Collections.sort(tupleList, new Comparator<Tuple2<Point, Float>>() {
                            @Override
                            public int compare(Tuple2<Point, Float> o1, Tuple2<Point, Float> o2) {
                                return o1.f1.compareTo(o2.f1);
                            }
                        });
                        while(count <k){
                            collector.collect(tupleList.get(count).f0);
                            count++;
                        }
                    }
                });
        return result;
    }

}
