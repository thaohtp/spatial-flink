package flink;

import flink.datatype.*;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.*;

/**
 * Created by JML on 5/11/17.
 */
public class IndexBuilder implements Serializable{

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
        builder.buildIndex(data, 2, 3, 0.5, 4);
    }


    // the idea of index builder is
    // Step 1: build STRPartition to get MBRs and split data into partitions based on STRPartition
    // Step 2: Get all partitioned data and build local index by map partition
    // Step 3: Build up global tree index

    public void buildIndex(DataSet<Point> data, final int nbDimension, final int nbNodePerEntry, double sampleRate, int parallelism) throws Exception {
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
        DataSet<RTree> localRTree = partitionedData.mapPartition(new MapPartitionFunction<Point, RTree>() {
            @Override
            public void mapPartition(Iterable<Point> iterable, Collector<RTree> collector) throws Exception {
                List<Point> pointList = new ArrayList<Point>();
                Iterator<Point> pointIter = iterable.iterator();
                while (pointIter.hasNext()){
                    pointList.add(pointIter.next());
                }

                STRPacking strPacking = new STRPacking(nbNodePerEntry,nbDimension);
                RTree rtree = strPacking.createRTree(pointList);
                collector.collect(rtree);
            }
        });

        // Step 3: build global RTree
        DataSet<RTree> globalRTree = localRTree.reduceGroup(new GroupReduceFunction<RTree, RTree>() {
            @Override
            public void reduce(Iterable<RTree> iterable, Collector<RTree> collector) throws Exception {
                Iterator<RTree> rtreeIter = iterable.iterator();
                int i =0;
                List<PartitionedMBR> partitionedMBRList = new ArrayList<PartitionedMBR>();
                while(rtreeIter.hasNext()){
                    i++;
                    RTree rtree = rtreeIter.next();
                    PartitionedMBR point = new PartitionedMBR(rtree.getRootNode().getMbr(), i);
                    partitionedMBRList.add(point);
                }

                STRPacking strPacking = new STRPacking(nbNodePerEntry, nbDimension);
                RTree globalTree = strPacking.createGlobalRTree(partitionedMBRList);
                collector.collect(globalTree);
            }
        });

        // TODO: Remove here after testing
        globalRTree.print();
        localRTree.print();

    }

    public STRPartitioner createSTRPartitioner(DataSet<Point> data, final int nbDimension, final int maxNodePerEntry, double sampleRate, final int parallelism) throws Exception {
        // Sample data
        boolean withReplacement = false;
        DataSet<Point> sampleData = DataSetUtils.sample(data, withReplacement, sampleRate);

        DataSet<RTree> rTrees = sampleData.reduceGroup(new RichGroupReduceFunction<Point, RTree>() {
            @Override
            public void reduce(Iterable<Point> iterable, Collector<RTree> collector) throws Exception {
                List<Point> samplePoints = new ArrayList<Point>();
                Iterator<Point> iter = iterable.iterator();
                while (iter.hasNext()) {
                    samplePoints.add(iter.next());
                }

                System.out.println("Sample size: " + samplePoints.size() );

                // calculate number nodes per entry
                int maxNumberMBR = parallelism;
                double nbNodePerEntry = Math.ceil(samplePoints.size() / maxNumberMBR);
                if(nbNodePerEntry == 0 ){
                    nbNodePerEntry = 1;
                }

                // Sort and pack points to create MBRs
                List<SliceIndex> sliceIndexList = sortPoints(samplePoints, 0, samplePoints.size(), nbNodePerEntry, 0, nbDimension, nbDimension);
                List<MBR> mbrBounds = new ArrayList<MBR>();
                for(int i = 0; i < sliceIndexList.size(); i++){
                    SliceIndex slice = sliceIndexList.get(i);
                    List<Point> subList = samplePoints.subList(slice.getStartIndex(), slice.getEndIndex());
                    List<MBR> mbrs = calculateMBR(subList, nbDimension, nbNodePerEntry);
                    mbrBounds.addAll(mbrs);
                }

                // From MBR, create partition points (MBR, partition). The idea is to distribute each MBR to each partition
                List<PartitionedMBR> partitionedMBRs = new ArrayList<PartitionedMBR>(mbrBounds.size());
                for(int i =0; i<mbrBounds.size(); i++){
                    // TODO: i% MBR to make sure, partitionnumber is not out of index bound
                    PartitionedMBR mbr = new PartitionedMBR(mbrBounds.get(i), i % maxNumberMBR);
                    partitionedMBRs.add(mbr);
                }
                RTree rTree = createGlobalRTree(partitionedMBRs, nbDimension, maxNodePerEntry);
                collector.collect(rTree);
            }
        });

        return new STRPartitioner(rTrees.collect().get(0));
    }

    /**
     * Sort given points into slabs
     * @param points data points
     * @param startIndex index of start point
     * @param endIndex index of end point (end index is exclusive)
     * @param nbPointsPerNode capacity of one node
     * @param currentDimension dimension which is used to compare points
     * @param nbDimension number of dimensions in points
     * @param k recursively dimension to divide into slabs
     * @return List of SliceIndex
     */
    private List<SliceIndex> sortPoints(List<Point> points, int startIndex, int endIndex, double nbPointsPerNode, final int currentDimension, int nbDimension, int k){

        // reach final dimension then stop sorting recursively
        if(currentDimension == nbDimension){
            List<SliceIndex> sliceIndexList = new ArrayList<SliceIndex>();
            sliceIndexList.add(new SliceIndex(startIndex, endIndex));
            return sliceIndexList;
        }

        // Calculate all information for r, n, S, P
        int r = endIndex - startIndex;
        double n = nbPointsPerNode;
        double P = r * 1.0/n;
        double power = 1.0 * (k-1) / k;
        int slabSize =(int) Math.ceil(n * Math.ceil(Math.pow(P, power)));
        int nbSlabs = (int) Math.ceil(r * 1.0 /slabSize);
//        int nbSlabs = (int) Math.ceil(Math.pow(P, 1.0/k));
//        int slabSize = (int) Math.floor(r / nbSlabs);

        // Sort the range
        List<Point> subList = points.subList(startIndex, endIndex);
        Collections.sort(subList, new Comparator<Point>() {
            @Override
            public int compare(Point o1, Point o2) {
                return o1.compare(o2, currentDimension);
            }
        });

        // Divide range into slabs
        List<SliceIndex> sliceIndices = new ArrayList<SliceIndex>();
        for(int i =0; i < nbSlabs - 1; i++){
            int startSubIndex = startIndex + i * slabSize;
            int endSubIndex = startIndex + (i+1) * slabSize;
            sliceIndices.addAll(sortPoints(points, startSubIndex, endSubIndex, nbPointsPerNode, currentDimension +1, nbDimension, k-1));
        }

        // Calculate start-end index of final slab
        int finalStartSubIndex = startIndex;
        if(sliceIndices.size() != 0){
            finalStartSubIndex = sliceIndices.get(sliceIndices.size() -1).getEndIndex();
        }
        sliceIndices.addAll(sortPoints(points, finalStartSubIndex, endIndex, nbPointsPerNode, currentDimension +1, nbDimension, k-1));

        return sliceIndices;
    }

    /**
     * This method is used for calculating MBR after sorting. This method does not pack points into node.
     * @param points
     * @param nbDimension
     * @param nbPointPerNode
     * @return
     * @throws Exception
     */
    private List<MBR> calculateMBR(List<Point> points, int nbDimension, double nbPointPerNode) throws Exception {
        int nbMBR = (int) Math.ceil(points.size() * 1.0/nbPointPerNode);
        List<MBR> result = new ArrayList<MBR>();
        for(int i =0; i<nbMBR - 1; i++){
            // calculate start-end index to pack nodes into MBR
            double startIndex = i * nbPointPerNode;
            double endIndex = (i+1) * nbPointPerNode;

            MBR mbr = new MBR(nbDimension);

            for(double j = startIndex; j<endIndex; j++){
                Point childPoint = points.get((int)j);
                mbr.addPoint(childPoint);
            }
            result.add(mbr);
        }

        // pack n-th slice
        int startIndex = result.size();
        MBR finalMBR = new MBR(nbDimension);
        for(int i = startIndex; i< points.size(); i++){
            Point childNode = points.get(i);
            finalMBR.addPoint(childNode);
        }
        result.add(finalMBR);

        return result;
    }

    private List<RTreeNode> packMBRLeafNodes(List<PartitionedMBR> points, int nbDimension, int nbPointPerNode, boolean isLeaf) throws Exception {
        int nbMBR = (int) Math.ceil(points.size() * 1.0/nbPointPerNode);
        List<RTreeNode> result = new ArrayList<RTreeNode>();
        for(int i =0; i<nbMBR - 1; i++){
            // calculate start-end index to pack nodes into MBR
            int startIndex = i * nbPointPerNode;
            int endIndex = (i+1) * nbPointPerNode;

            RTreeNode parentNode = null;
            if(isLeaf){
                parentNode = new MBRLeafNode(nbDimension);
            }
            else{
                parentNode = new NonLeafNode(nbDimension);
            }

            PartitionedMBR startPoint = points.get(startIndex);
            parentNode.insert(startPoint);
            parentNode.setMbr(startPoint.getMbr());

            for(int j = startIndex + 1; j<endIndex; j++){
                PartitionedMBR childPoint = points.get(j);
                parentNode.insert(childPoint);
            }
            result.add(parentNode);
        }

        // pack n-th slice
        int startIndex = result.size();
        RTreeNode finalParentNode = null;
        if(isLeaf){
            finalParentNode = new MBRLeafNode(nbDimension);
        }
        else{
            finalParentNode = new NonLeafNode(nbDimension);
        }
        PartitionedMBR startNode = points.get(startIndex);
        finalParentNode.insert(startNode);
        finalParentNode.setMbr(startNode.getMbr());

        for(int i = startIndex+1; i< points.size(); i++){
            PartitionedMBR childNode = points.get(i);
            finalParentNode.insert(childNode);
        }
        result.add(finalParentNode);

        return result;
    }

    /**
     * This method is used to pack global Rtree. Global RTree contains Entry<MBR,Partition number> instead of Points.
     * @param mbrList
     * @param nbDimension
     * @param pointPerNode
     * @return
     * @throws Exception
     */
    public RTree createGlobalRTree(List<PartitionedMBR> mbrList, int nbDimension, int pointPerNode) throws Exception {
        // 1. Pack points into leaf node
        List<SliceIndex> sliceIndexList = sortPartitionedMBRs(mbrList, 0, mbrList.size(), pointPerNode, 0, nbDimension, nbDimension);
        List<RTreeNode> leafNodes = new ArrayList<RTreeNode>();
        for(int i = 0; i < sliceIndexList.size(); i++){
            SliceIndex slice = sliceIndexList.get(i);
            List<PartitionedMBR> subList = mbrList.subList(slice.getStartIndex(), slice.getEndIndex());
            List<RTreeNode> treeNodes = packMBRLeafNodes(subList, nbDimension, pointPerNode, true);
            leafNodes.addAll(treeNodes);
        }

        // 2. Pack leaf nodes into non-leaf nodes. Pack bottom - up until we get only one root node
        List<RTreeNode> nonLeafNode = null;
        do{
            List<SliceIndex> sliceIndexOfNodes = sortRTreeNodes(leafNodes, 0, leafNodes.size(), pointPerNode, 0, nbDimension, nbDimension);
            nonLeafNode = new ArrayList<RTreeNode>();
            for(int i = 0; i < sliceIndexOfNodes.size(); i++){
                SliceIndex slice = sliceIndexOfNodes.get(i);
                List<RTreeNode> subList = leafNodes.subList(slice.getStartIndex(), slice.getEndIndex());
                List<RTreeNode> treeNodes = packRTreeNodes(subList, nbDimension, pointPerNode, nbDimension-1, false);
                nonLeafNode.addAll(treeNodes);
            }
            leafNodes = nonLeafNode;
        } while(nonLeafNode.size() != 1);

        return new RTree(nonLeafNode.get(0));
    }



    /**
     * Sort given points into slabs
     * @param mbrList data points
     * @param startIndex index of start point
     * @param endIndex index of end point (end index is exclusive)
     * @param nbPointsPerNode capacity of one node
     * @param currentDimension dimension which is used to compare points
     * @param nbDimension number of dimensions in points
     * @param k recursively dimension to divide into slabs
     * @return List of SliceIndex
     */
    private List<SliceIndex> sortPartitionedMBRs(List<PartitionedMBR> mbrList, int startIndex, int endIndex, int nbPointsPerNode, final int currentDimension, int nbDimension, int k){

        // reach final dimension then stop sorting recursively
        if(currentDimension == nbDimension){
            List<SliceIndex> sliceIndexList = new ArrayList<SliceIndex>();
            sliceIndexList.add(new SliceIndex(startIndex, endIndex));
            return sliceIndexList;
        }

        // Calculate all information for r, n, S, P
        int r = endIndex - startIndex;
        int n = nbPointsPerNode;
        double P = r * 1.0/n;
        double power = 1.0 * (k-1) / k;
        int slabSize =(int) Math.ceil(n * Math.ceil(Math.pow(P, power)));
        int nbSlabs = (int) Math.ceil(r * 1.0 /slabSize);
//        int nbSlabs = (int) Math.ceil(Math.pow(P, 1.0/k));
//        int slabSize = (int) Math.floor(r / nbSlabs);

        // Sort the range
        List<PartitionedMBR> subList = mbrList.subList(startIndex, endIndex);
        Collections.sort(subList, new Comparator<PartitionedMBR>() {
            @Override
            public int compare(PartitionedMBR o1, PartitionedMBR o2) {
                return o1.getMbr().compare(o2.getMbr(), currentDimension);
            }
        });

        // Divide range into slabs
        List<SliceIndex> sliceIndices = new ArrayList<SliceIndex>();
        for(int i =0; i < nbSlabs - 1; i++){
            int startSubIndex = startIndex + i * slabSize;
            int endSubIndex = startIndex + (i+1) * slabSize;
            sliceIndices.addAll(sortPartitionedMBRs(mbrList, startSubIndex, endSubIndex, nbPointsPerNode, currentDimension +1, nbDimension, k-1));
        }

        // Calculate start-end index of final slab
        int finalStartSubIndex = startIndex;
        if(sliceIndices.size() != 0){
            finalStartSubIndex = sliceIndices.get(sliceIndices.size() -1).getEndIndex();
        }
        sliceIndices.addAll(sortPartitionedMBRs(mbrList, finalStartSubIndex, endIndex, nbPointsPerNode, currentDimension +1, nbDimension, k-1));

        return sliceIndices;
    }


    /**
     * Packing list of nodes into buckets of nodes, each bucket contains nbPointPerNode nodes. Points are sorted by compared dimension
     * @param nodes list of nodes
     * @param nbPointPerNode number of points per node
     * @param compareDim compared dimension
     * @param isLeaf flag for packing points in leaf level
     * @return
     */
    private List<RTreeNode> packRTreeNodes(List<RTreeNode> nodes, int nbDimension, int nbPointPerNode, final int compareDim, boolean isLeaf) throws Exception {
        int nbMBR = (int) Math.ceil(nodes.size() * 1.0/nbPointPerNode);
        List<RTreeNode> result = new ArrayList<RTreeNode>();
        // only pack until reaching (n-1)th slices
        for(int i =0; i<nbMBR - 1; i++){
            // calculate start-end index to pack nodes into MBR
            int startIndex = i * nbPointPerNode;
            int endIndex = (i+1) * nbPointPerNode;

            RTreeNode parentNode = null;
            if(isLeaf){
                parentNode = new PointLeafNode(nbDimension);
            }
            else{
                parentNode = new NonLeafNode(nbDimension);
            }
            RTreeNode startNode = nodes.get(startIndex);
            parentNode.insert(startNode);
            parentNode.setMbr(startNode.getMbr());

            for(int j = startIndex +1; j<endIndex; j++){
                RTreeNode childNode = nodes.get(j);
                parentNode.insert(childNode);
            }
            result.add(parentNode);
        }

        // pack n-th slice
        int startIndex = result.size();
        RTreeNode finalParentNode = null;
        if(isLeaf){
            finalParentNode = new PointLeafNode(nbDimension);
        }
        else{
            finalParentNode = new NonLeafNode(nbDimension);
        }
        RTreeNode startNode = nodes.get(startIndex);
        finalParentNode.insert(startNode);
        finalParentNode.setMbr(startNode.getMbr());

        for(int i = startIndex +1; i< nodes.size(); i++){
            RTreeNode childNode = nodes.get(i);
            finalParentNode.insert(childNode);
        }
        result.add(finalParentNode);

        return result;
    }


    /**
     * Sort given nodes into slabs
     * @param nodes RTree nodes
     * @param startIndex index of start node
     * @param endIndex index of end node (end index is exclusive)
     * @param nbPointsPerNode capacity of one node
     * @param currentDimension dimension which is used to compare nodes
     * @param nbDimension number of dimensions in points
     * @param k recursively dimension to divide into slabs
     * @return
     */
    private List<SliceIndex> sortRTreeNodes(List<RTreeNode> nodes, int startIndex, int endIndex, int nbPointsPerNode, final int currentDimension, int nbDimension, int k){
        // reach final dimension then stop sorting recursively
        if(currentDimension == nbDimension){
            List<SliceIndex> sliceIndexList = new ArrayList<SliceIndex>();
            sliceIndexList.add(new SliceIndex(startIndex, endIndex));
            return sliceIndexList;
        }

        // Calculate all information for r, n, S, P
        int r = endIndex - startIndex;
        int n = nbPointsPerNode;
        double P = r * 1.0/n;
        double power = 1.0 * (k-1) / k;
        int slabSize =(int) Math.ceil(n * Math.ceil(Math.pow(P, power)));
        int nbSlabs = (int) Math.ceil(r * 1.0 /slabSize);
//        int nbSlabs = (int) Math.ceil(Math.pow(P, 1.0/k));
//        int slabSize = (int) Math.floor(r / nbSlabs);

        // Sort the range
        List<RTreeNode> subList = nodes.subList(startIndex, endIndex);
        Collections.sort(subList, new Comparator<RTreeNode>() {
            @Override
            public int compare(RTreeNode o1, RTreeNode o2) {
                return o1.getMbr().compare(o2.getMbr(), currentDimension);
            }
        });

        // Divide into slabs
        List<SliceIndex> sliceIndices = new ArrayList<SliceIndex>();
        for(int i =0; i < nbSlabs - 1; i++){
            int startSubIndex = startIndex + i * slabSize;
            int endSubIndex = startIndex + (i+1) * slabSize;
            sliceIndices.addAll(sortRTreeNodes(nodes, startSubIndex, endSubIndex, nbPointsPerNode, currentDimension +1, nbDimension, k-1));
        }

        // Calculate start-end index of final slab
        int finalStartSubIndex = startIndex;
        if(sliceIndices.size() != 0){
            finalStartSubIndex = sliceIndices.get(sliceIndices.size() -1).getEndIndex();
        }
        sliceIndices.addAll(sortRTreeNodes(nodes, finalStartSubIndex, endIndex, nbPointsPerNode, currentDimension +1, nbDimension, k-1));

        return sliceIndices;
    }


}
