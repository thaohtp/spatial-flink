package de.tu_berlin.dima.util;

import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.datatype.*;
import org.apache.flink.api.common.io.BinaryOutputFormat;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.List;

/**
 * Created by JML on 7/28/17.
 */
public class RTreeBinayOutputFormat extends BinaryOutputFormat<RTree>{

    @Override
    protected void serialize(RTree rTree, DataOutputView dataOutputView) throws IOException {
        this.writeRTreeNode(rTree.getRootNode(), dataOutputView);
    }

    public void writeRTreeNode(RTreeNode rTreeNode, DataOutputView output) throws IOException {
        if(rTreeNode instanceof NonLeafNode){
            output.writeBoolean(rTreeNode.isLeaf());
            this.writeMBR(rTreeNode.getMbr(), output);
            output.writeInt(rTreeNode.getNbDimension());
            List<RTreeNode> childNodes = rTreeNode.getChildNodes();
            output.writeInt(childNodes.size());
            for(int i =0; i< childNodes.size(); i++){
                this.writeRTreeNode(childNodes.get(i), output);
            }
        }
        else{
            output.writeBoolean(rTreeNode.isLeaf());
            this.writeMBR(rTreeNode.getMbr(), output);
            output.writeInt(rTreeNode.getNbDimension());
            if(rTreeNode instanceof PointLeafNode){
                output.writeBoolean(true);
                PointLeafNode leaf = (PointLeafNode) rTreeNode;
                output.writeInt(leaf.getEntries().size());
                for(int i =0; i< leaf.getEntries().size(); i++){
                    this.writePoint(leaf.getEntries().get(i), output);
                }
            }
            else{
                output.writeBoolean(false);
                MBRLeafNode leaf = (MBRLeafNode) rTreeNode;
                output.writeInt(leaf.getEntries().size());
                for(int i =0; i< leaf.getEntries().size(); i++){
                    this.writePartitionedMBR(leaf.getEntries().get(i), output);
                }
            }
        }
    }

    public void writePoint(Point point, DataOutputView output) throws IOException {
        output.writeInt(point.getNbDimension());
        for(int i =0; i<point.getNbDimension(); i++){
            output.writeFloat(point.getDimension(i));
        }
    }

    public void writeMBR(MBR mbr, DataOutputView output) throws IOException {
        this.writePoint(mbr.getMaxPoint(), output);
        this.writePoint(mbr.getMinPoint(), output);
        output.writeBoolean(mbr.isInitialized());
        output.writeInt(mbr.getNbDimension());
    }


    public void writePartitionedMBR(PartitionedMBR partitionedMBR, DataOutputView output) throws IOException {
        this.writeMBR(partitionedMBR.getMbr(), output);
        output.writeInt(partitionedMBR.getPartitionNumber());
    }
}
