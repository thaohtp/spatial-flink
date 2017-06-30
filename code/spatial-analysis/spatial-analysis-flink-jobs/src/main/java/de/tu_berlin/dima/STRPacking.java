package de.tu_berlin.dima;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import de.tu_berlin.dima.datatype.*;

/**
 * Created by JML on 3/7/17.
 */
public class STRPacking {
    int pointPerNode;
    int nbDimension;

    public STRPacking(int nbPointPerNode, int nbDimension){
        this.pointPerNode = nbPointPerNode;
        this.nbDimension = nbDimension;
    }

//    public void sort(){
//        // sort by first dimension
//
//        for(int i =0; i< nbDimension; i++){
//            // calculate dimension: dimension is varies from 1 to n
//            int k = i + 1;
//
//            // calculate P = r/n with r = size of dataset, n = number of points per node
//            int P =0;
//            if(this.pointList.size() % this.pointPerNode == 0 ){
//                P = this.pointList.size() /this.pointPerNode;
//            }
//            else{
//                P = this.pointList.size() /this.pointPerNode + 1;
//            }
//
//            int S = 1;
//            if(k != 1){
//                float temp = 1L/(k-1);
//                S = (int) Math.pow(P, temp);
//            }
//
//            // sort start index, sort end index
//            int pointPerSlab = this.pointList.size();
//            if(S != 1){
//                pointPerSlab = (int) Math.ceil(Math.sqrt(this.pointList.size() / (S-1)));
//            }
//            for(int j =0; j < S; j++){
//                int startIndex = pointPerSlab * j;
//                int endIndex = Math.min(pointPerSlab * (j+1) -1, this.pointList.size());
//                List<Point> subList = new ArrayList<Point>();
//                for(int m = startIndex; m <= endIndex; m++){
//                    subList.add(this.pointList.remove(startIndex));
//                }
//                Collections.sort(subList, new PointComparator(k-1));
//                for(int m = startIndex; m <= endIndex; m++) {
//                    pointList.add(m, subList.remove(0));
//                }
//            }
//
//            // sort those point
//        }
//    }

    public RTree createRTree(List<Point> pointList) throws Exception {
        // 1. Pack points into leaf node
        List<SliceIndex> sliceIndexList = sortPoints(pointList, 0, pointList.size(), this.pointPerNode, 0, this.nbDimension, this.nbDimension);
        List<RTreeNode> leafNodes = new ArrayList<RTreeNode>();
        for(int i = 0; i < sliceIndexList.size(); i++){
            SliceIndex slice = sliceIndexList.get(i);
            List<Point> subList = pointList.subList(slice.getStartIndex(), slice.getEndIndex());
            List<RTreeNode> treeNodes = packLeafNode(subList, this.pointPerNode, this.nbDimension-1, true);
            leafNodes.addAll(treeNodes);
        }

        // 2. Pack leaf nodes into non-leaf nodes. Pack bottom - up until we get only one root node
        List<RTreeNode> nonLeafNode = null;
        do{
            List<SliceIndex> sliceIndexOfNodes = sortRTreeNode(leafNodes, 0, leafNodes.size(), this.pointPerNode, 0, this.nbDimension, this.nbDimension);
            nonLeafNode = new ArrayList<RTreeNode>();
            for(int i = 0; i < sliceIndexOfNodes.size(); i++){
                SliceIndex slice = sliceIndexOfNodes.get(i);
                List<RTreeNode> subList = leafNodes.subList(slice.getStartIndex(), slice.getEndIndex());
                List<RTreeNode> treeNodes = packRTreeNodes(subList, this.pointPerNode, this.nbDimension-1, false);
                nonLeafNode.addAll(treeNodes);
            }
            leafNodes = nonLeafNode;
        } while(nonLeafNode.size() != 1);

        return new RTree(nonLeafNode.get(0));
    }


    public RTree createGlobalRTree(List<PartitionedMBR> mbrList) throws Exception {
        // 1. Pack points into leaf node
        List<SliceIndex> sliceIndexList = sortPartitionedMBRs(mbrList, 0, mbrList.size(), this.pointPerNode, 0, this.nbDimension, this.nbDimension);
        List<RTreeNode> leafNodes = new ArrayList<RTreeNode>();
        for(int i = 0; i < sliceIndexList.size(); i++){
            SliceIndex slice = sliceIndexList.get(i);
            List<PartitionedMBR> subList = mbrList.subList(slice.getStartIndex(), slice.getEndIndex());
            List<RTreeNode> treeNodes = packMBRLeafNode(subList, this.pointPerNode, this.nbDimension-1, true);
            leafNodes.addAll(treeNodes);
        }

        // 2. Pack leaf nodes into non-leaf nodes. Pack bottom - up until we get only one root node
        List<RTreeNode> nonLeafNode = null;
        do{
            List<SliceIndex> sliceIndexOfNodes = sortRTreeNode(leafNodes, 0, leafNodes.size(), this.pointPerNode, 0, this.nbDimension, this.nbDimension);
            nonLeafNode = new ArrayList<RTreeNode>();
            for(int i = 0; i < sliceIndexOfNodes.size(); i++){
                SliceIndex slice = sliceIndexOfNodes.get(i);
                List<RTreeNode> subList = leafNodes.subList(slice.getStartIndex(), slice.getEndIndex());
                List<RTreeNode> treeNodes = packRTreeNodes(subList, this.pointPerNode, this.nbDimension-1, false);
                nonLeafNode.addAll(treeNodes);
            }
            leafNodes = nonLeafNode;
        } while(nonLeafNode.size() != 1);

        return new RTree(nonLeafNode.get(0));
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
    private List<SliceIndex> sortRTreeNode(List<RTreeNode> nodes, int startIndex, int endIndex, int nbPointsPerNode, final int currentDimension, int nbDimension, int k){
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
            sliceIndices.addAll(sortRTreeNode(nodes, startSubIndex, endSubIndex, nbPointsPerNode, currentDimension +1, nbDimension, k-1));
        }

        // Calculate start-end index of final slab
        int finalStartSubIndex = startIndex;
        if(sliceIndices.size() != 0){
            finalStartSubIndex = sliceIndices.get(sliceIndices.size() -1).getEndIndex();
        }
        sliceIndices.addAll(sortRTreeNode(nodes, finalStartSubIndex, endIndex, nbPointsPerNode, currentDimension +1, nbDimension, k-1));

        return sliceIndices;
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
    private List<SliceIndex> sortPoints(List<Point> points, int startIndex, int endIndex, int nbPointsPerNode, final int currentDimension, int nbDimension, int k){

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
    private List<RTreeNode> packRTreeNodes(List<RTreeNode> nodes, int nbPointPerNode, final int compareDim, boolean isLeaf) throws Exception {
        int nbMBR = (int) Math.ceil(nodes.size() * 1.0/nbPointPerNode);
        List<RTreeNode> result = new ArrayList<RTreeNode>();
        // only pack until reaching (n-1)th slices
        for(int i =0; i<nbMBR - 1; i++){
            // calculate start-end index to pack nodes into MBR
            int startIndex = i * nbPointPerNode;
            int endIndex = (i+1) * nbPointPerNode;

            RTreeNode parentNode = null;
            if(isLeaf){
                parentNode = new PointLeafNode(this.nbDimension);
            }
            else{
                parentNode = new NonLeafNode(this.nbDimension);
            }

            for(int j = startIndex; j<endIndex; j++){
                RTreeNode childNode = nodes.get(j);
                parentNode.insert(childNode);
            }
            result.add(parentNode);
        }

        // pack n-th slice
        int startIndex = result.size();
        RTreeNode finalParentNode = null;
        if(isLeaf){
            finalParentNode = new PointLeafNode(this.nbDimension);
        }
        else{
            finalParentNode = new NonLeafNode(this.nbDimension);
        }
        for(int i = startIndex; i< nodes.size(); i++){
            RTreeNode childNode = nodes.get(i);
            finalParentNode.insert(childNode);
        }
        result.add(finalParentNode);

        return result;
    }


    // TODO: refactor here, not good solution
    private List<RTreeNode> packLeafNode(List<Point> points, int nbPointPerNode, final int compareDim, boolean isLeaf) throws Exception {
        int nbMBR = (int) Math.ceil(points.size() * 1.0/nbPointPerNode);
        List<RTreeNode> result = new ArrayList<RTreeNode>();
        for(int i =0; i<nbMBR - 1; i++){
            // calculate start-end index to pack nodes into MBR
            int startIndex = i * nbPointPerNode;
            int endIndex = (i+1) * nbPointPerNode;

            RTreeNode parentNode = null;
            if(isLeaf){
                parentNode = new PointLeafNode(this.nbDimension);
            }
            else{
                parentNode = new NonLeafNode(this.nbDimension);
            }

            for(int j = startIndex; j<endIndex; j++){
                Point childPoint = points.get(j);
                parentNode.insert(childPoint);
            }
            result.add(parentNode);
        }

        // pack n-th slice
        int startIndex = result.size();
        RTreeNode finalParentNode = null;
        if(isLeaf){
            finalParentNode = new PointLeafNode(this.nbDimension);
        }
        else{
            finalParentNode = new NonLeafNode(this.nbDimension);
        }
        for(int i = startIndex; i< points.size(); i++){
            Point childNode = points.get(i);
            finalParentNode.insert(childNode);
        }
        result.add(finalParentNode);

        return result;
    }


    // TODO: refactor here, not good solution
    private List<RTreeNode> packMBRLeafNode(List<PartitionedMBR> mbrList, int nbPointPerNode, final int compareDim, boolean isLeaf) throws Exception {
        int nbMBR = (int) Math.ceil(mbrList.size() * 1.0/nbPointPerNode);
        List<RTreeNode> result = new ArrayList<RTreeNode>();
        for(int i =0; i<nbMBR - 1; i++){
            // calculate start-end index to pack nodes into MBR
            int startIndex = i * nbPointPerNode;
            int endIndex = (i+1) * nbPointPerNode;

            RTreeNode parentNode = null;
            if(isLeaf){
                parentNode = new MBRLeafNode(this.nbDimension);
            }
            else{
                parentNode = new NonLeafNode(this.nbDimension);
            }

            for(int j = startIndex; j<endIndex; j++){
                PartitionedMBR childPoint = mbrList.get(j);
                parentNode.insert(childPoint);
            }
            result.add(parentNode);
        }

        // pack n-th slice
        int startIndex = result.size();
        RTreeNode finalParentNode = null;
        if(isLeaf){
            finalParentNode = new MBRLeafNode(this.nbDimension);
        }
        else{
            finalParentNode = new NonLeafNode(this.nbDimension);
        }
        for(int i = startIndex; i< mbrList.size(); i++){
            PartitionedMBR childNode = mbrList.get(i);
            finalParentNode.insert(childNode);
        }
        result.add(finalParentNode);

        return result;
    }

}
