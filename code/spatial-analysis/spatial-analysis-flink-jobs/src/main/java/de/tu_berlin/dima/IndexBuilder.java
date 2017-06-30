package de.tu_berlin.dima;

import de.tu_berlin.dima.datatype.*;
import de.tu_berlin.dima.test.IndexBuilderResult;
import de.tu_berlin.dima.util.PointComparator;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

/**
 * Created by JML on 5/11/17.
 */
public class IndexBuilder implements Serializable {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Point> data = env.fromElements(
                new Tuple2<Integer, Integer>(1, 1), new Tuple2<Integer, Integer>(1, 2),
                new Tuple2<Integer, Integer>(2, 3), new Tuple2<Integer, Integer>(1020, 3),
                new Tuple2<Integer, Integer>(45, 3), new Tuple2<Integer, Integer>(40, 3),
                new Tuple2<Integer, Integer>(-1, 5), new Tuple2<Integer, Integer>(0, -1))
                .map(new MapFunction<Tuple2<Integer, Integer>, Point>() {
                    @Override
                    public Point map(Tuple2<Integer, Integer> tuple) throws Exception {
                        List<Float> floatList = new ArrayList<Float>(2);
                        floatList.add(0, (float) tuple.f0);
                        floatList.add(1, (float) tuple.f1);
                        return new Point(floatList);
                    }
                });

        IndexBuilder builder = new IndexBuilder();
        IndexBuilderResult result = builder.buildIndex(data, 2, 3, 0.5, 4);

        System.out.println("After partitioning partitioner ");
        List<RTreeNode> leafNodes = result.getPartitioner().getrTree().getLeafNodes();
        for (RTreeNode node : leafNodes) {
            MBRLeafNode leaf = (MBRLeafNode) node;
            List<PartitionedMBR> entries = leaf.getEntries();
            for (PartitionedMBR mbr : entries) {
                System.out.println(mbr.getMbr().toString());
            }
        }

        System.out.println("RTree in partitioner: ");
        System.out.println(result.getPartitioner().getrTree());

    }

    // the idea of index builder is
    // Step 1: build STRPartition to get MBRs and split data into partitions based on STRPartition
    // Step 2: Get all partitioned data and build local index by map partition
    // Step 3: Build up global tree index

    public IndexBuilderResult buildIndex(DataSet<Point> data, final int nbDimension, final int nbNodePerEntry, final double sampleRate, int parallelism) throws Exception {
        // Step 1: create MBR and STRPartitioner based on sampled data
        boolean withReplacement = false;
        STRPartitioner partitioner = createSTRPartitioner(data, nbDimension, nbNodePerEntry, sampleRate, parallelism);
        // Partition data
        DataSet<Point> partitionedData = data.partitionCustom(partitioner, new KeySelector<Point, Point>() {
            @Override
            public Point getKey(Point point) throws Exception {
                return point;
            }
        });

        // Step 2: build local RTree
        DataSet<RTree> localRTree = partitionedData.mapPartition(new RichMapPartitionFunction<Point, RTree>() {
            @Override
            public void mapPartition(Iterable<Point> iterable, Collector<RTree> collector) throws Exception {
                List<Point> points = new ArrayList<Point>();
                Iterator<Point> pointIter = iterable.iterator();
                while (pointIter.hasNext()) {
                    points.add(pointIter.next());
                }

                if(!points.isEmpty()){
                    RTree rTree = createLocalRTree(points, nbDimension, nbNodePerEntry);
                    collector.collect(rTree);
                }
            }
        });

        // Step 3: build global RTree
        DataSet<RTree> globalRTree = localRTree
                .map(new RichMapFunction<RTree, Tuple2<Integer, RTree>>() {
                    @Override
                    public Tuple2<Integer, RTree> map(RTree rTree) throws Exception {
                        return new Tuple2<Integer, RTree>(getRuntimeContext().getIndexOfThisSubtask(), rTree);
                    }
                })
                .reduceGroup(new RichGroupReduceFunction<Tuple2<Integer, RTree>, RTree>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Integer, RTree>> iterable, Collector<RTree> collector) throws Exception {
                        Iterator<Tuple2<Integer, RTree>> iter = iterable.iterator();
                        List<PartitionedMBR> partitionedMBRList = new ArrayList<PartitionedMBR>();
                        while (iter.hasNext()) {
                            Tuple2<Integer, RTree> tuple = iter.next();
                            RTree rtree = tuple.f1;
                            PartitionedMBR point = new PartitionedMBR(rtree.getRootNode().getMbr(), tuple.f0);
                            point.setSize(rtree.getRootNode().getSize());
                            partitionedMBRList.add(point);
                        }

                        RTree globalTree = createGlobalRTree(partitionedMBRList, nbDimension, nbNodePerEntry);
                        collector.collect(globalTree);
                    }
                });

        return new IndexBuilderResult(partitionedData, globalRTree, localRTree, partitioner);
    }


    /**
     * This method is used for creating STRPartitioner based on sampled data.
     *
     * @param data
     * @param nbDimension
     * @param maxNodePerEntry
     * @param sampleRate
     * @param parallelism
     * @return
     * @throws Exception
     */
    public STRPartitioner createSTRPartitioner(DataSet<Point> data, final int nbDimension, final int maxNodePerEntry, double sampleRate, final int parallelism) throws Exception {
        // create boundary for whole dataset
        // Tuple2<MBR, number of data points>
        DataSet<Tuple2<MBR, Integer>> globalBoundDS = data
                .map(new MapFunction<Point, Tuple2<MBR, Integer>>() {
                    @Override
                    public Tuple2<MBR, Integer> map(Point point) throws Exception {
                        MBR mbr = new MBR(nbDimension);
                        float[] cloneVals = new float[point.getNbDimension()];
                        for(int i = 0; i < cloneVals.length; i++){
                            cloneVals[i] = point.getDimension(i);
                        }
                        Point clonePoint = new Point(cloneVals);
                        mbr.setMaxPoint(point);
                        mbr.setMinPoint(clonePoint);
                        mbr.setInitialized(false);
                        return new Tuple2<MBR, Integer>(mbr, 0);
                    }
                })
                .reduce(new ReduceFunction<Tuple2<MBR, Integer>>() {
                    @Override
                    public Tuple2<MBR, Integer> reduce(Tuple2<MBR, Integer> mbrIntegerTuple2, Tuple2<MBR, Integer> t1) throws Exception {
                        mbrIntegerTuple2.f0.addPoint(t1.f0.getMinPoint());
                        mbrIntegerTuple2.f1++;
                        return mbrIntegerTuple2;
                    }
                });

//        final Tuple2<MBR, Integer> globalBound = globalBoundDS.collect().get(0);

        // calculate number of MBRs on each dimension
        // the idea is the total number of mbr over all dimensions will be equal to parallelism
        // remaining = P in the algorithm
        double remaining = parallelism;
        final int[] nbMBRs = new int[nbDimension];
        for (int i = 0; i < nbDimension; i++) {
            // nbMBR = S in each dimension
            nbMBRs[i] = (int) Math.ceil(Math.pow(remaining, 1.0 / (nbDimension - i)));
            remaining = remaining * 1.0 / nbMBRs[i];
        }

        // Sample data
        boolean withReplacement = false;
        final DataSet<Point> sampleData = DataSetUtils.sample(data, withReplacement, sampleRate);

        DataSet<RTree> rTrees = sampleData.reduceGroup(new RichGroupReduceFunction<Point, RTree>() {
            private MBR globalBound;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.globalBound = ((Tuple2<MBR,Integer>)this.getRuntimeContext().getBroadcastVariable("globalBoundDS").get(0)).f0;

            }

            @Override
            public void reduce(Iterable<Point> iterable, Collector<RTree> collector) throws Exception {
                List<Point> samplePoints = new ArrayList<Point>();
                Iterator<Point> iter = iterable.iterator();
                while (iter.hasNext()) {
                    samplePoints.add(iter.next());
                }

                // calculate number nodes per entry
                float[] lowerBounds = new float[nbDimension];
                float[] upperBounds = new float[nbDimension];
                List<MBR> mbrBounds = packPointsIntoMBR(samplePoints, nbMBRs, lowerBounds, upperBounds, nbDimension, 0, this.globalBound);

                // From MBR, create partition points (MBR, partition). The idea is to distribute each MBR to each partition
                List<PartitionedMBR> partitionedMBRs = new ArrayList<PartitionedMBR>(mbrBounds.size());
                for (int i = 0; i < mbrBounds.size(); i++) {
                    // TODO: i% MBR to make sure, partitionnumber is not out of index bound
                    PartitionedMBR mbr = new PartitionedMBR(mbrBounds.get(i), i % parallelism);
                    mbr.setSize(mbrBounds.get(i).getSize());
                    partitionedMBRs.add(mbr);
                }
                RTree rTree = createGlobalRTree(partitionedMBRs, nbDimension, maxNodePerEntry);
                collector.collect(rTree);
            }
        }).withBroadcastSet(globalBoundDS, "globalBoundDS");

        return new STRPartitioner(rTrees.collect().get(0));
    }

    public List<MBR> packPointsIntoMBR(List<Point> data, int[] nbMBRs, float[] lowerBounds, float[] upperBounds, int nbDimension, int currentDimension, MBR globalBound) {
        // sort data first
        Collections.sort(data, new PointComparator(currentDimension));
        int currentNbMBR = nbMBRs[currentDimension];
        int nbPointPerMBR = (int) Math.ceil(data.size() * 1.0 / currentNbMBR);

        List<MBR> result = new ArrayList<MBR>();
        if (currentDimension == nbDimension -1 ) {
            // For each group i, we create a MBR
            for (int i = 0; i < currentNbMBR; i++) {
                // calculate index of group in data
                int startIndex = i * nbPointPerMBR;
                int endIndex = (i + 1) * nbPointPerMBR -1;

                if (i == 0) {
                    // for group 0, lower bound = min bound of global data
                    // upper bound = last element of group
                    lowerBounds[currentDimension] = globalBound.getMinPoint().getDimension(currentDimension);
                    upperBounds[currentDimension] = data.get(endIndex).getDimension(currentDimension);
                } else {
                    if (i < currentNbMBR - 1) {
                        // for middle group, lower bound = upper bound of previous group
                        // upper bound = last element of group
                        lowerBounds[currentDimension] = data.get(startIndex - 1).getDimension(currentDimension);
                        upperBounds[currentDimension] = data.get(endIndex).getDimension(currentDimension);
                    } else {
                        // for last group, lower bound = upper bound of previous group
                        // upper bound = max bound of global data
                        endIndex = data.size()-1;
                        lowerBounds[currentDimension] = data.get(startIndex - 1).getDimension(currentDimension);
                        upperBounds[currentDimension] = globalBound.getMaxPoint().getDimension(currentDimension);
                    }
                }

                MBR mbr = new MBR(new Point(lowerBounds), new Point(upperBounds));
                mbr.setSize(endIndex - startIndex +1);
                mbr.setInitialized(false);
                result.add(mbr);
            }
        } else {
            float[] cloneUpperBounds = upperBounds.clone();
            float[] cloneLowerBounds = lowerBounds.clone();

            // For each group i, we create a MBR
            for (int i = 0; i < currentNbMBR; i++) {
                // calculate index of group in data
                int startIndex = i * nbPointPerMBR;
                int endIndex = (i + 1) * nbPointPerMBR -1;

                if (i == 0) {
                    // for group 0, lower bound = min bound of global data
                    // upper bound = last element of group
                    cloneLowerBounds[currentDimension] = globalBound.getMinPoint().getDimension(currentDimension);
                    cloneUpperBounds[currentDimension] = data.get(endIndex).getDimension(currentDimension);
                } else {
                    if (i < currentNbMBR - 1) {
                        // for middle group, lower bound = upper bound of previous group
                        // upper bound = last element of group
                        cloneLowerBounds[currentDimension] = data.get(startIndex - 1).getDimension(currentDimension);
                        cloneUpperBounds[currentDimension] = data.get(endIndex).getDimension(currentDimension);
                    } else {
                        // for last group, lower bound = upper bound of previous group
                        // upper bound = max bound of global data
                        endIndex = data.size()-1;
                        cloneLowerBounds[currentDimension] = data.get(startIndex - 1).getDimension(currentDimension);
                        cloneUpperBounds[currentDimension] = globalBound.getMaxPoint().getDimension(currentDimension);
                    }
                }
                //  we use endIndex +1 because endIndex is exclusive in sublist
                List<Point> group = new ArrayList<Point>(data.subList(startIndex, endIndex +1));
                List<MBR> mbrs = packPointsIntoMBR(group, nbMBRs, cloneLowerBounds, cloneUpperBounds, nbDimension, currentDimension + 1, globalBound);
                result.addAll(mbrs);
            }
        }
        return result;
    }


    public List<MBRLeafNode> packMBRLeafNodes(List<PartitionedMBR> data, int[] totalLeaf, int nbDimension, final int currentDimension) {
        // sort data first
        Collections.sort(data, new Comparator<PartitionedMBR>() {
            @Override
            public int compare(PartitionedMBR o1, PartitionedMBR o2) {
                return o1.getMbr().compare(o2.getMbr(), currentDimension);
            }
        });
        int curTotalLeaf = totalLeaf[currentDimension];
        int maxPointPerEntry = (int) Math.ceil(data.size() * 1.0 / curTotalLeaf);

        List<MBRLeafNode> result = new ArrayList<MBRLeafNode>();
        if (currentDimension == nbDimension -1) {
            // For each group i, we create a MBR
            for (int i = 0; i < curTotalLeaf; i++) {
                // calculate index of group in data
                int startIndex = i * maxPointPerEntry;
                int endIndex = (i + 1) * maxPointPerEntry;
                if(i == curTotalLeaf - 1){
                    endIndex = data.size();
                }

                MBRLeafNode leafNode = new MBRLeafNode(nbDimension);
                for(int j = startIndex; j< endIndex; j++){
                    leafNode.addPoint(data.get(j));
                }
                result.add(leafNode);
            }
        } else {

            // For each group i, we create a MBR
            for (int i = 0; i < curTotalLeaf; i++) {
                // calculate index of group in data
                int startIndex = i * maxPointPerEntry;
                int endIndex = (i + 1) * maxPointPerEntry;

                if(i == curTotalLeaf -1){
                    endIndex = data.size();
                }

                //  we use endIndex +1 because endIndex is exclusive in sublist
                List<PartitionedMBR> group = data.subList(startIndex, endIndex);
                List<MBRLeafNode> leafNodes = packMBRLeafNodes(group, totalLeaf, nbDimension, currentDimension + 1);
                result.addAll(leafNodes);
            }
        }
        return result;
    }

    public List<PointLeafNode> packPointLeafNodes(List<Point> data, int[] totalLeaf, int nbDimension, final int currentDimension) {
        // sort data first
        Collections.sort(data, new PointComparator(currentDimension));
        int maxPointPerEntry = (int) Math.ceil(data.size() * 1.0 / totalLeaf[currentDimension]);
        int curTotalLeaf = (int) Math.ceil(data.size() * 1.0 / maxPointPerEntry);

        List<PointLeafNode> result = new ArrayList<PointLeafNode>();
        if (currentDimension == nbDimension -1) {
            // For each group i, we create a MBR
            for (int i = 0; i < curTotalLeaf; i++) {
                // calculate index of group in data
                int startIndex = i * maxPointPerEntry;
                int endIndex = (i + 1) * maxPointPerEntry;
                if(i == curTotalLeaf - 1){
                    endIndex = data.size();
                }

                PointLeafNode leafNode = new PointLeafNode(nbDimension);
                for(int j = startIndex; j< endIndex; j++){
                    leafNode.addPoint(data.get(j));
                }
                result.add(leafNode);
            }
        } else {

            // For each group i, we create a MBR
            for (int i = 0; i < curTotalLeaf; i++) {
                // calculate index of group in data
                int startIndex = i * maxPointPerEntry;
                int endIndex = (i + 1) * maxPointPerEntry;

                if(i == curTotalLeaf -1){
                    endIndex = data.size();
                }

                //  we use endIndex +1 because endIndex is exclusive in sublist
                List<Point> group = data.subList(startIndex, endIndex);
                List<PointLeafNode> leafNodes = packPointLeafNodes(group, totalLeaf, nbDimension, currentDimension + 1);
                result.addAll(leafNodes);
            }
        }
        return result;
    }

    public List<RTreeNode> packRTreeNodes(List<RTreeNode> data, int[] nbMBRs, int nbDimension, final int currentDimension) {
        // sort data first
        Collections.sort(data, new Comparator<RTreeNode>() {
            @Override
            public int compare(RTreeNode o1, RTreeNode o2) {
                return o1.getMbr().compare(o2.getMbr(), currentDimension);
            }
        });

        int nbPointPerMBR = (int) Math.ceil(data.size() * 1.0 / nbMBRs[currentDimension]);
        int currentNbMBR = (int) Math.ceil(data.size()*1.0 / nbPointPerMBR);

        List<RTreeNode> result = new ArrayList<RTreeNode>();
        if (currentDimension == nbDimension -1) {
            // For each group i, we create a MBR
            for (int i = 0; i < currentNbMBR; i++) {
                // calculate index of group in data
                int startIndex = i * nbPointPerMBR;
                int endIndex = (i + 1) * nbPointPerMBR;
                if(i == currentNbMBR - 1){
                    endIndex = data.size();
                }

                NonLeafNode node = new NonLeafNode(nbDimension);
                for(int j = startIndex; j< endIndex; j++){
                    try {
                        node.insert(data.get(j));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                result.add(node);
            }
        } else {

            // For each group i, we create a MBR
            for (int i = 0; i < currentNbMBR; i++) {
                // calculate index of group in data
                int startIndex = i * nbPointPerMBR;
                int endIndex = (i + 1) * nbPointPerMBR;

                if(i == currentNbMBR -1){
                    endIndex = data.size();
                }

                //  we use endIndex +1 because endIndex is exclusive in sublist
                List<RTreeNode> group = data.subList(startIndex, endIndex);
                List<RTreeNode> leafNodes = packRTreeNodes(group, nbMBRs, nbDimension, currentDimension + 1);
                result.addAll(leafNodes);
            }
        }
        return result;
    }

    /**
     * This method is used to pack global Rtree. Global RTree contains Entry<MBR,Partition number> instead of Points.
     *
     * @param mbrList
     * @param nbDimension
     * @param maxPointPerEntry
     * @return
     * @throws Exception
     */
    public RTree createGlobalRTree(List<PartitionedMBR> mbrList, int nbDimension, int maxPointPerEntry) throws Exception {
        // calculate number of nodes we have
        int totalLeaf = (int) Math.ceil(mbrList.size() * 1.0 / maxPointPerEntry);
        int[] nbLeaf = calculateSlabQuantity(totalLeaf, nbDimension);

        // 2. Pack leaf nodes into non-leaf nodes. Pack bottom - up until we get only one root node
        List<RTreeNode> nodes = (List) packMBRLeafNodes(mbrList, nbLeaf, nbDimension, 0);
        do {
            int totalEntry = (int) Math.ceil(nodes.size() *1.0 / maxPointPerEntry);
            int[] nbSlabs = calculateSlabQuantity(totalEntry, nbDimension);
            nodes = packRTreeNodes(nodes, nbSlabs, nbDimension, 0);

        } while (nodes.size() != 1);
        return new RTree(nodes.get(0));
    }

    /**
     * This method is used to pack global Rtree. Global RTree contains Entry<MBR,Partition number> instead of Points.
     *
     * @param points
     * @param nbDimension
     * @param maxPointPerEntry
     * @return
     * @throws Exception
     */
    public RTree createLocalRTree(List<Point> points, int nbDimension, int maxPointPerEntry) throws Exception {
        // calculate number of nodes we have
        int totalLeaf = (int) Math.ceil(points.size() * 1.0 / maxPointPerEntry);
        int[] nbLeaf = calculateSlabQuantity(totalLeaf, nbDimension);

        // 2. Pack leaf nodes into non-leaf nodes. Pack bottom - up until we get only one root node
        List<RTreeNode> nodes = (List) packPointLeafNodes(points, nbLeaf, nbDimension, 0);
        do {
            int totalEntry = (int) Math.ceil(nodes.size() *1.0 / maxPointPerEntry);
            int[] nbSlabs = calculateSlabQuantity(totalEntry, nbDimension);
            nodes = packRTreeNodes(nodes, nbSlabs, nbDimension, 0);

        } while (nodes.size() != 1);
        return new RTree(nodes.get(0));
    }


    private int[] calculateSlabQuantity(int total, int nbDimension){
        double remaining = total;
        final int[] quantity = new int[nbDimension];
        for (int i = 0; i < nbDimension; i++) {
            // nbMBR = S in each dimension
            quantity[i] = (int) Math.ceil(Math.pow(remaining, 1.0 / (nbDimension - i)));
            remaining = remaining * 1.0 / quantity[i];
        }
        return quantity;
    }

    /**
     * This version is added for testing only. It requires sampleData as a parameter to make sure the test will always return same result.
     *
     * @param data
     * @param nbDimension
     * @param nbNodePerEntry
     * @param sampleRate
     * @param parallelism
     * @throws Exception
     */
    public IndexBuilderResult buildIndexTestVersion(DataSet<Point> data, DataSet<Point> sampleData, final int nbDimension, final int nbNodePerEntry, double sampleRate, int parallelism) throws Exception {
        // Step 1: create MBR and STRPartitioner based on sampled data
        boolean withReplacement = false;
        STRPartitioner partitioner = createSTRPartitioner(data, nbDimension, nbNodePerEntry, sampleRate, parallelism);

        List<RTreeNode> leafNodes = partitioner.getrTree().getLeafNodes();
        for (RTreeNode node : leafNodes) {
            MBRLeafNode leaf = (MBRLeafNode) node;
            List<PartitionedMBR> entries = leaf.getEntries();
        }

        // Partition data
        DataSet<Point> partitionedData = data.partitionCustom(partitioner, new KeySelector<Point, Point>() {
            @Override
            public Point getKey(Point point) throws Exception {
                return point;
            }
        });

        // Step 2: build local RTree
        DataSet<RTree> localRTree = partitionedData.mapPartition(new RichMapPartitionFunction<Point, RTree>() {
            @Override
            public void mapPartition(Iterable<Point> iterable, Collector<RTree> collector) throws Exception {
                List<Point> points = new ArrayList<Point>();
                Iterator<Point> pointIter = iterable.iterator();
                while (pointIter.hasNext()) {
                    points.add(pointIter.next());
                }

                if(!points.isEmpty()){
                    RTree rTree = createLocalRTree(points, nbDimension, nbNodePerEntry);
                    collector.collect(rTree);
                }
            }
        });

        // Step 3: build global RTree
        DataSet<RTree> globalRTree = localRTree
                .map(new RichMapFunction<RTree, Tuple2<Integer, RTree>>() {
                    @Override
                    public Tuple2<Integer, RTree> map(RTree rTree) throws Exception {
                        return new Tuple2<Integer, RTree>(getRuntimeContext().getIndexOfThisSubtask(), rTree);
                    }
                })
                .reduceGroup(new RichGroupReduceFunction<Tuple2<Integer, RTree>, RTree>() {
                    @Override
                    public void reduce(Iterable<Tuple2<Integer, RTree>> iterable, Collector<RTree> collector) throws Exception {
                        Iterator<Tuple2<Integer, RTree>> iter = iterable.iterator();
                        List<PartitionedMBR> partitionedMBRList = new ArrayList<PartitionedMBR>();
                        while (iter.hasNext()) {
                            Tuple2<Integer, RTree> tuple = iter.next();
                            RTree rtree = tuple.f1;
                            PartitionedMBR point = new PartitionedMBR(rtree.getRootNode().getMbr(), tuple.f0);
                            point.setSize(rtree.getRootNode().getSize());
                            partitionedMBRList.add(point);
                        }

                        RTree globalTree = createGlobalRTree(partitionedMBRList, nbDimension, nbNodePerEntry);
                        collector.collect(globalTree);
                    }
                });
        return new IndexBuilderResult(partitionedData, globalRTree, localRTree, partitioner);
    }


    /**
     * This version is added for testing only. It requires sampleData as a parameter to make sure the test will always return same result.
     *
     * @param data
     * @param sampleData
     * @param nbDimension
     * @param maxNodePerEntry
     * @param sampleRate
     * @param parallelism
     * @return
     * @throws Exception
     */
    public STRPartitioner createSTRPartitionerTestVersion(DataSet<Point> data, DataSet<Point> sampleData, final int nbDimension, final int maxNodePerEntry, double sampleRate, final int parallelism) throws Exception {
        // create boundary for whole dataset
        // Tuple2<MBR, number of data points>
        DataSet<Tuple2<MBR, Integer>> globalBoundDS = data
                .map(new MapFunction<Point, Tuple2<MBR, Integer>>() {
                    @Override
                    public Tuple2<MBR, Integer> map(Point point) throws Exception {
                        MBR mbr = new MBR(nbDimension);
                        float[] cloneVals = new float[point.getNbDimension()];
                        for(int i = 0; i < cloneVals.length; i++){
                            cloneVals[i] = point.getDimension(i);
                        }
                        Point clonePoint = new Point(cloneVals);
                        mbr.setMaxPoint(point);
                        mbr.setMinPoint(clonePoint);
                        mbr.setInitialized(false);
                        return new Tuple2<MBR, Integer>(mbr, 0);
                    }
                })
                .reduce(new ReduceFunction<Tuple2<MBR, Integer>>() {
                    @Override
                    public Tuple2<MBR, Integer> reduce(Tuple2<MBR, Integer> mbrIntegerTuple2, Tuple2<MBR, Integer> t1) throws Exception {
                        mbrIntegerTuple2.f0.addPoint(t1.f0.getMinPoint());
                        mbrIntegerTuple2.f1++;
                        return mbrIntegerTuple2;
                    }
                });

        final Tuple2<MBR, Integer> globalBound = globalBoundDS.collect().get(0);

        // calculate number of MBRs on each dimension
        // the idea is the total number of mbr over all dimensions will be equal to parallelism
        // remaining = P in the algorithm
        double remaining = parallelism;
        final int[] nbMBRs = new int[nbDimension];
        for (int i = 0; i < nbDimension; i++) {
            // nbMBR = S in each dimension
            nbMBRs[i] = (int) Math.ceil(Math.pow(remaining, 1.0 / (nbDimension - i)));
            remaining = remaining * 1.0 / nbMBRs[i];
        }

        // Sample data
        boolean withReplacement = false;

        DataSet<RTree> rTrees = sampleData.reduceGroup(new RichGroupReduceFunction<Point, RTree>() {
            @Override
            public void reduce(Iterable<Point> iterable, Collector<RTree> collector) throws Exception {
                List<Point> samplePoints = new ArrayList<Point>();
                Iterator<Point> iter = iterable.iterator();
                while (iter.hasNext()) {
                    samplePoints.add(iter.next());
                }

                // calculate number nodes per entry
                float[] lowerBounds = new float[nbDimension];
                float[] upperBounds = new float[nbDimension];
                List<MBR> mbrBounds = packPointsIntoMBR(samplePoints, nbMBRs, lowerBounds, upperBounds, nbDimension, 0, globalBound.f0);

                // From MBR, create partition points (MBR, partition). The idea is to distribute each MBR to each partition
                List<PartitionedMBR> partitionedMBRs = new ArrayList<PartitionedMBR>(mbrBounds.size());
                for (int i = 0; i < mbrBounds.size(); i++) {
                    // TODO: i% MBR to make sure, partitionnumber is not out of index bound
                    PartitionedMBR mbr = new PartitionedMBR(mbrBounds.get(i), i % parallelism);
                    mbr.setSize(mbrBounds.get(i).getSize());
                    partitionedMBRs.add(mbr);
                }
                RTree rTree = createGlobalRTree(partitionedMBRs, nbDimension, maxNodePerEntry);
                collector.collect(rTree);
            }
        });

        return new STRPartitioner(rTrees.collect().get(0));
    }

}
