package de.tu_berlin.dima.util;

import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.datatype.*;
import org.apache.flink.api.common.io.BinaryInputFormat;
import org.apache.flink.core.memory.DataInputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 7/28/17.
 */
public class RTreeBinaryInputFormat extends BinaryInputFormat<RTree>{
    @Override
    protected RTree deserialize(RTree rTree, DataInputView dataInputView) throws IOException {
        NonLeafNode root = (NonLeafNode) this.readRTreeNode(dataInputView);
        RTree tree = new RTree(root);
        tree.setNumBytes(root.getNumBytes());
        return tree;
    }

    public Point readPoint(DataInputView input) throws IOException {
        int numDimension = input.readInt();
        float[] vals = new float[numDimension];
        for(int i =0; i< numDimension; i++){
            vals[i] = input.readFloat();
        }
        Point p = new Point(vals);
        p.setNumbBytes( 4 * (numDimension +1));
        return p;
    }

    public MBR readMBR(DataInputView input) throws IOException {
        Point maxPoint = this.readPoint(input);
        Point minPoint = this.readPoint(input);
        MBR mbr = new MBR(maxPoint, minPoint);
        mbr.setInitialized(input.readBoolean());
        mbr.setNbDimension(input.readInt());
        mbr.setNumBytes(maxPoint.getNumbBytes() + minPoint.getNumbBytes() + 1 + 4);
        return mbr;
    }

    public PartitionedMBR readPartitionedMBR(DataInputView input) throws IOException {
        MBR mbr = this.readMBR(input);
        int partitionNumber = input.readInt();
        PartitionedMBR partitionedMBR = new PartitionedMBR(mbr, partitionNumber);
        partitionedMBR.setNumBytes(4 + mbr.getNumBytes());
        return partitionedMBR;
    }

    public RTreeNode readRTreeNode(DataInputView input) throws IOException {
        long numBytes = 0;
        boolean isLeaf = input.readBoolean();
        numBytes += 1;

        if(!isLeaf){
            RTreeNode node = null;
            // Read NonLeafNode
            MBR mbr = this.readMBR(input);
            numBytes += mbr.getNumBytes();
            int numDimension = input.readInt();

            int numChildNodes = input.readInt();
            numBytes += 8;
            List<RTreeNode> childNodes = new ArrayList<RTreeNode>(numChildNodes);
            for(int i = 0; i< numChildNodes; i++){
                RTreeNode child = this.readRTreeNode(input);
                numBytes += child.getNumBytes();
                childNodes.add(child);
            }
            node = new NonLeafNode(numDimension, mbr, childNodes);

            node.setNumBytes(numBytes);
            return node;
        }
        else{
            // Read PointLeafNode or MBRLeafNode
            LeafNode node = null;
            MBR mbr = this.readMBR(input);
            numBytes += mbr.getNumBytes();
            int numDimension = input.readInt();

            boolean isPointLeaf = input.readBoolean();
            numBytes += 5;
            LeafNode leafNode = null;
            if(isPointLeaf){
                int numEntries = input.readInt();
                numBytes += 4;
                List<Point> points = new ArrayList<Point>(numEntries);
                for(int i = 0; i< numEntries; i++){
                    Point point = this.readPoint(input);
                    points.add(point);
                    numBytes += point.getNumbBytes();
                }
                node = new PointLeafNode(numDimension);
                node.setEntries(points);

            }
            else{
                int numEntries = input.readInt();
                numBytes += 4;
                List<PartitionedMBR> partitionedMBRS = new ArrayList<PartitionedMBR>(numEntries);
                for(int i = 0; i< numEntries; i++){
                    PartitionedMBR partitionedMBR = this.readPartitionedMBR(input);
                    partitionedMBRS.add(partitionedMBR);
                    numBytes += partitionedMBR.getNumBytes();
                }
                node = new MBRLeafNode(numDimension);
                node.setEntries(partitionedMBRS);
            }

            node.setNbDimension(numDimension);
            node.setMbr(mbr);

            node.setNumBytes(numBytes);
            return node;
        }
    }

}
