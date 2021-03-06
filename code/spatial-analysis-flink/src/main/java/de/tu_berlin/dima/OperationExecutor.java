package de.tu_berlin.dima;

import de.tu_berlin.dima.datatype.*;
import de.tu_berlin.dima.test.IndexBuilderResult;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Created by JML on 6/27/17.
 */
public class OperationExecutor {
    private int numDimension;
    private int maxNodePerEntry;
    private int parallelism;
    private float sampleRate;
    private IndexBuilder indexBuilder;

    public OperationExecutor(int numDimension, int maxNodePerEntry, int parallelism, float sampleRate){
        this.numDimension = numDimension;
        this.maxNodePerEntry = maxNodePerEntry;
        this.parallelism = parallelism;
        this.sampleRate = sampleRate;
        this.indexBuilder = new IndexBuilder();
    }


    public DataSet<Point> boxRangeQuery(final MBR box, DataSet<Point> data) throws Exception {
        IndexBuilderResult indexBuilderResult = this.indexBuilder.buildIndex(data, this.numDimension, this.maxNodePerEntry, this.sampleRate, this.parallelism);
        DataSet<RTree> globalTree = indexBuilderResult.getGlobalRTree();
        DataSet<Point> partitionedData = indexBuilderResult.getData();
        return this.boxRangeQuery(box, partitionedData, globalTree);

    }

    public DataSet<Point> boxRangeQuery(final MBR box, DataSet<Point> partitionedData, DataSet<RTree> globalTree) {
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
                        if (box.contains(point)) {
                            collector.collect(point);
                        }
                    }
                });
        return result;
    }

    public DataSet<Point> boxRangeQuery(final MBR box, DataSet<Point> partitionedData, DataSet<RTree> globalTree, DataSet<RTree> localTrees) {
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

        DataSet<Point> result = localTrees
                .flatMap(new RichFlatMapFunction<RTree, Point>() {
                    @Override
                    public void flatMap(RTree rTree, Collector<Point> collector) throws Exception {
                        List<Integer> partitionFlags = getRuntimeContext().getBroadcastVariable("partitionFlags");
                        if (partitionFlags.contains(getRuntimeContext().getIndexOfThisSubtask())) {
                            List<Point> points = rTree.boxRange(box);
                            for(int i = 0; i< points.size(); i++){
                                collector.collect(points.get(i));
                            }
                        }
                    }
                }).withBroadcastSet(partitionFlags, "partitionFlags");
        return result;
    }

    public DataSet<Point> kNNQuery(final Point queryPoint, final Integer k, DataSet<Point> partitionedData) {
        DataSet<Point> result = partitionedData.map(new RichMapFunction<Point, Tuple2<Point, Double>>() {
            @Override
            public Tuple2<Point, Double> map(Point point) throws Exception {
                Double distance = point.calcDistance(queryPoint);
                return new Tuple2<Point, Double>(point, distance);
            }
        })
                .sortPartition(1, Order.ASCENDING)
                .mapPartition(new RichMapPartitionFunction<Tuple2<Point, Double>, Tuple2<Point, Double>>() {
                    @Override
                    public void mapPartition(Iterable<Tuple2<Point, Double>> iterable, Collector<Tuple2<Point, Double>> collector) throws Exception {
                        int count = 0;
                        Iterator<Tuple2<Point, Double>> iter = iterable.iterator();
                        while (iter.hasNext() && count < k) {
                            Tuple2<Point, Double> tuple = iter.next();
                            collector.collect(tuple);
                            count++;
                        }
                    }
                })
                .reduceGroup(new RichGroupReduceFunction<Tuple2<Point, Double>, Point>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Point, Double>> iterable, Collector<Point> collector) throws Exception {
                        Iterator<Tuple2<Point, Double>> iter = iterable.iterator();
                        List<Tuple2<Point, Double>> tupleList = new ArrayList<Tuple2<Point, Double>>();
                        int count = 0;

                        while (iter.hasNext()) {
                            tupleList.add(iter.next());
                        }
                        Collections.sort(tupleList, new Comparator<Tuple2<Point, Double>>() {
                            @Override
                            public int compare(Tuple2<Point, Double> o1, Tuple2<Point, Double> o2) {
                                return o1.f1.compareTo(o2.f1);
                            }
                        });
                        while (count < k) {
                            collector.collect(tupleList.get(count).f0);
                            count++;
                        }
                    }
                });
        return result;
    }

    public DataSet<Point> circleRangeQuery(final Point queryPoint, final Float radius, DataSet<Point> data) throws Exception {
        IndexBuilderResult indexBuilderResult = this.indexBuilder.buildIndex(data, this.numDimension, this.maxNodePerEntry, this.sampleRate, this.parallelism);
        DataSet<RTree> globalTree = indexBuilderResult.getGlobalRTree();
        DataSet<Point> partitionedData = indexBuilderResult.getData();
        return this.circleRangeQuery(queryPoint, radius, partitionedData, globalTree);
    }

    public DataSet<Point> circleRangeQuery(final Point queryPoint, final Float radius, DataSet<Point> partitionedData, DataSet<RTree> globalTree) {
        DataSet<Integer> partitionFlags = globalTree.flatMap(new RichFlatMapFunction<RTree, PartitionedMBR>() {
            @Override
            public void flatMap(RTree rTree, Collector<PartitionedMBR> collector) throws Exception {
                List<PartitionedMBR> partitionedMBRS = rTree.search(queryPoint, radius);
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
                                Point p = pointIter.next();
                                collector.collect(p);
                            }
                        }

                    }
                }).withBroadcastSet(partitionFlags, "partitionFlags")
                .flatMap(new FlatMapFunction<Point, Point>() {
                    @Override
                    public void flatMap(Point point, Collector<Point> collector) throws Exception {
                        if (queryPoint.calcDistance(point) <= radius) {
                            collector.collect(point);
                        }
                    }
                });
        return result;
    }


    public DataSet<Point> circleRangeQuery(final Point queryPoint, final Float radius, DataSet<Point> partitionedData, DataSet<RTree> globalTree, DataSet<RTree> localTrees) {
        DataSet<Integer> partitionFlags = globalTree.flatMap(new RichFlatMapFunction<RTree, PartitionedMBR>() {
            @Override
            public void flatMap(RTree rTree, Collector<PartitionedMBR> collector) throws Exception {
                List<PartitionedMBR> partitionedMBRS = rTree.search(queryPoint, radius);
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

        DataSet<Point> result = localTrees.flatMap(new RichFlatMapFunction<RTree, Point>() {
            @Override
            public void flatMap(RTree rTree, Collector<Point> collector) throws Exception {
                List<Integer> partitionFlags = getRuntimeContext().getBroadcastVariable("partitionFlags");
                if (partitionFlags.contains(getRuntimeContext().getIndexOfThisSubtask())) {
                    List<Point> result = rTree.circleRange(queryPoint, radius);
                    for(int i =0; i< result.size(); i++){
                       collector.collect(result.get(i));
                    }
                }
            }
        }).withBroadcastSet(partitionFlags, "partitionFlags");
        return result;
    }


    public DataSet<JoinedRow> distanceJoin(final float distance, DataSet<Point> left, final DataSet<Point> right) throws Exception {
        IndexBuilderResult leftResult = this.indexBuilder.buildIndex(left, this.numDimension, this.maxNodePerEntry, this.sampleRate, this.parallelism);
        DataSet<RTree> leftGlobalTree = leftResult.getGlobalRTree();
        DataSet<Point> leftPartitionedData = leftResult.getData();
        DataSet<Point> rightPartitionedData = this.indexBuilder.partition(right, this.numDimension, this.maxNodePerEntry, this.sampleRate, this.parallelism);
        return this.distanceJoin(distance, leftPartitionedData, leftGlobalTree, rightPartitionedData);

    }
    public DataSet<JoinedRow> distanceJoin(final float distance, DataSet<Point> left, DataSet<RTree> leftTree, final DataSet<Point> right) {
        // build index for one data set
        DataSet<Tuple2<Integer, Point>> right_joined_partition = right
                .flatMap(new RichFlatMapFunction<Point, Tuple2<Integer, Point>>() {
                    RTree tree;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        this.tree = (RTree) getRuntimeContext().getBroadcastVariable("leftTree").get(0);
                    }

                    @Override
                    public void flatMap(Point point, Collector<Tuple2<Integer, Point>> collector) throws Exception {
                        List<PartitionedMBR> mbrs = this.tree.search(point, distance);
                        for (int i = 0; i < mbrs.size(); i++) {
                            int partition = mbrs.get(i).getPartitionNumber();
                            collector.collect(new Tuple2<Integer, Point>(partition, point));
                        }
                    }
                })
                .withBroadcastSet(leftTree, "leftTree");

        DataSet<JoinedRow> result = left.map(new RichMapFunction<Point, Tuple2<Integer, Point>>() {
            @Override
            public Tuple2<Integer, Point> map(Point point) throws Exception {
                return new Tuple2<Integer, Point>(getRuntimeContext().getIndexOfThisSubtask(), point);
            }
        })
                .coGroup(right_joined_partition)
                .where(0)
                .equalTo(0)
                .with(new DistanceJoinCoGroupFunc(this.indexBuilder, this.numDimension, this.maxNodePerEntry, distance));

        // find the partition first then join
        return result;
    }

    public DataSet<Tuple2<Point, Point>> kNNJoin(final int k, final DataSet<Point> left, DataSet<Point> right) throws Exception {
        IndexBuilderResult leftResult = this.indexBuilder.buildIndex(left, this.numDimension, this.maxNodePerEntry, this.sampleRate, this.parallelism);
        DataSet<Point> leftPartitioned = leftResult.getData();
        DataSet<RTree> leftTree = leftResult.getGlobalRTree();
        DataSet<Point> rightPartitioned = this.indexBuilder.partition(right, this.numDimension, this.maxNodePerEntry, this.sampleRate, this.parallelism);
        return this.kNNJoin(k, leftPartitioned, leftTree, rightPartitioned);
    }

    public DataSet<Tuple2<Point, Point>> kNNJoin(final int k, final DataSet<Point> leftPartitioned, final DataSet<RTree> leftTree, final DataSet<Point> rightPartitioned) throws Exception {
        DataSet<Point> rightSample = DataSetUtils.sample(rightPartitioned, false, this.sampleRate);
        // Find the center and maxDistance for each MBR of Left
        DataSet<Tuple3<Integer, Point, Double>> leftPartitionedBounds = leftTree
                .flatMap(new FlatMapFunction<RTree, Tuple3<Integer, Point, Double>>() {
                    @Override
                    public void flatMap(RTree rTree, Collector<Tuple3<Integer, Point, Double>> collector) throws Exception {
                        List<RTreeNode> leafNodes = rTree.getLeafNodes();
                        for(int i =0; i<leafNodes.size(); i++){
                            MBRLeafNode leaf = (MBRLeafNode) leafNodes.get(i);
                            for(int j =0; j<leaf.getEntries().size(); j++){
                                PartitionedMBR partitionedMBR = leaf.getEntries().get(j);
                                collector.collect(new Tuple3<Integer, Point, Double>(partitionedMBR.getPartitionNumber(), partitionedMBR.getMbr().getCenter(), partitionedMBR.getMbr().calcMaxDistanceFromCenter()));
                            }
                        }
                    }
                });

        // Build Rtree for sample data of Right
        DataSet<RTree> rightSampleTree = rightSample.reduceGroup(new SampleTreeGroupReduceFunc(this.indexBuilder, this.numDimension, this.maxNodePerEntry));

        // Build theta for each MBR of Left
        DataSet<Tuple4<Integer, Point, Double, Double>> thetaBound = leftPartitionedBounds
                .map(new RichMapFunction<Tuple3<Integer, Point, Double>, Tuple4<Integer, Point, Double, Double>>() {
                    RTree tree;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        this.tree = (RTree) getRuntimeContext().getBroadcastVariable("rightSampleTree").get(0);
                    }

                    @Override
                    public Tuple4<Integer, Point, Double, Double> map(Tuple3<Integer, Point, Double> tuple) throws Exception {
                        List<Double> distances = this.tree.kNNDistance(tuple.f1, k);
                        double theta = distances.get(distances.size()-1) + (tuple.f2 * 2);
                        return new Tuple4<Integer, Point, Double, Double>(tuple.f0, tuple.f1, tuple.f2, theta);
                    }
                })
                .withBroadcastSet(rightSampleTree, "rightSampleTree");

        DataSet<Tuple2<Integer, Point>> rightJoinPartitioned = rightPartitioned
                .flatMap(new RichFlatMapFunction<Point, Tuple2<Integer, Point>>() {
                    List<Tuple4<Integer, Point, Double, Double>> thetaBound;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        this.thetaBound = getRuntimeContext().getBroadcastVariable("thetaBound");
                    }

                    @Override
                    public void flatMap(Point point, Collector<Tuple2<Integer, Point>> collector) throws Exception {
                        Set<Integer> partitionSet = new HashSet<Integer>();
                        for(int i =0; i< thetaBound.size(); i++){
                            int partition = thetaBound.get(i).f0;
                            Point center = thetaBound.get(i).f1;
                            double bound = thetaBound.get(i).f3;
                            if(!partitionSet.contains(partition) && center.calcDistance(point) <= bound){
                                collector.collect(new Tuple2<Integer, Point>(partition, point));
                                partitionSet.add(partition);
                            }
                        }
                    }
                })
                .withBroadcastSet(thetaBound, "thetaBound");

        DataSet<Tuple2<Point, Point>> result = leftPartitioned.map(new RichMapFunction<Point, Tuple2<Integer, Point>>() {
            @Override
            public Tuple2<Integer, Point> map(Point point) throws Exception {
                return new Tuple2<Integer, Point>(getRuntimeContext().getIndexOfThisSubtask(), point);
            }
        })
                .coGroup(rightJoinPartitioned)
                .where(0)
                .equalTo(0)
                .with(new KNNJoinCoGroupFunc(this.indexBuilder, this.numDimension, this.maxNodePerEntry, k));
        return result;
    }

    private class SampleTreeGroupReduceFunc implements GroupReduceFunction<Point, RTree>{
        private IndexBuilder indexBuilder;
        private int numDimension;
        private int maxNodePerEntry;

        public SampleTreeGroupReduceFunc(IndexBuilder indexBuilder, int numDimension, int maxNodePerEntry){
            this.indexBuilder = indexBuilder;
            this.numDimension = numDimension;
            this.maxNodePerEntry =maxNodePerEntry;
        }

        @Override
        public void reduce(Iterable<Point> iterable, Collector<RTree> collector) throws Exception {
            Iterator<Point> iter = iterable.iterator();
            List<Point> data = new ArrayList<Point>();
            while (iter.hasNext()) {
                data.add(iter.next());
            }
            RTree tree = indexBuilder.createLocalRTree(data, numDimension, maxNodePerEntry);
            collector.collect(tree);
        }
    }

    private class KNNJoinCoGroupFunc implements CoGroupFunction<Tuple2<Integer, Point>, Tuple2<Integer, Point>, Tuple2<Point, Point>> {
        private IndexBuilder indexBuilder;
        private int numDimension;
        private int maxNodePerEntry;
        private int k;

        public KNNJoinCoGroupFunc(IndexBuilder indexBuilder, int numDimension, int maxNodePerEntry, int k){
            this.indexBuilder = indexBuilder;
            this.numDimension = numDimension;
            this.maxNodePerEntry =maxNodePerEntry;
            this.k = k;
        }

        @Override
        public void coGroup(Iterable<Tuple2<Integer, Point>> iterable, Iterable<Tuple2<Integer, Point>> iterable1, Collector<Tuple2<Point, Point>> collector) throws Exception {
            Iterator<Tuple2<Integer, Point>> leftIter = iterable.iterator();
            Iterator<Tuple2<Integer, Point>> rightIter = iterable1.iterator();
            List<Point> rightPoints = new ArrayList<Point>();
            while(rightIter.hasNext()){
                rightPoints.add(rightIter.next().f1);
            }
            if(!rightPoints.isEmpty()){
                RTree tree = indexBuilder.createLocalRTree(rightPoints, numDimension, maxNodePerEntry);
                while(leftIter.hasNext()){
                    Tuple2<Integer, Point> leftTuple = leftIter.next();
                    List<Point> kNNPoints = tree.kNN2(leftTuple.f1, k);
                    for(int i =0; i< kNNPoints.size(); i++){
                        collector.collect(new Tuple2<Point, Point>(leftTuple.f1, kNNPoints.get(i)));
                    }
                }
            }
        }
    }

    private class DistanceJoinCoGroupFunc implements CoGroupFunction<Tuple2<Integer, Point>, Tuple2<Integer, Point>, JoinedRow> {
        private IndexBuilder indexBuilder;
        private int numDimension;
        private int maxNodePerEntry;
        private float distance;

        public DistanceJoinCoGroupFunc(IndexBuilder indexBuilder, int numDimension, int maxNodePerEntry, float distance){
            this.indexBuilder = indexBuilder;
            this.numDimension = numDimension;
            this.maxNodePerEntry =maxNodePerEntry;
            this.distance = distance;
        }

        @Override
        public void coGroup(Iterable<Tuple2<Integer, Point>> iterable, Iterable<Tuple2<Integer, Point>> iterable1, Collector<JoinedRow> collector) throws Exception {
            Iterator<Tuple2<Integer, Point>> leftIter = iterable.iterator();
            Iterator<Tuple2<Integer, Point>> rightIter = iterable1.iterator();
            List<Point> rightPoints = new ArrayList<Point>();
            while(rightIter.hasNext()){
                rightPoints.add(rightIter.next().f1);
            }
            if(!rightPoints.isEmpty()){
                RTree tree = indexBuilder.createLocalRTree(rightPoints, numDimension, maxNodePerEntry);
                while(leftIter.hasNext()){
                    Tuple2<Integer, Point> leftTuple = leftIter.next();
                    List<Point> circlePoints = tree.circleRange(leftTuple.f1, distance);
                    for(int i =0; i< circlePoints.size(); i++){
                        collector.collect(new JoinedRow(leftTuple.f1, circlePoints.get(i)));
                    }
                }
            }
        }
    }
}
