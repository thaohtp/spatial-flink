package de.tu_berlin.dima.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.tu_berlin.dima.datatype.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 6/28/17.
 */
public class RTreeNodeSerializer extends Serializer<RTreeNode> {
    @Override
    public void write(Kryo kryo, Output output, RTreeNode object) {
        if(object instanceof NonLeafNode){
            output.writeBoolean(object.isLeaf());
            kryo.writeObject(output, object.getMbr());
            output.writeInt(object.getNbDimension());
            List<RTreeNode> childNodes = object.getChildNodes();
            output.writeInt(childNodes.size());
            for(int i =0; i< childNodes.size(); i++){
                kryo.writeObject(output, childNodes.get(i));
            }
        }
        else{
            output.writeBoolean(object.isLeaf());
            kryo.writeObject(output, object.getMbr());
            output.writeInt(object.getNbDimension());
            if(object instanceof PointLeafNode){
                output.writeBoolean(true);
                PointLeafNode leaf = (PointLeafNode) object;
                output.writeInt(leaf.getEntries().size());
                for(int i =0; i< leaf.getEntries().size(); i++){
                    kryo.writeObject(output, leaf.getEntries().get(i));
                }
            }
            else{
                output.writeBoolean(false);
                MBRLeafNode leaf = (MBRLeafNode) object;
                output.writeInt(leaf.getEntries().size());
                for(int i =0; i< leaf.getEntries().size(); i++){
                    kryo.writeObject(output, leaf.getEntries().get(i));
                }
            }
        }
    }

    @Override
    public RTreeNode read(Kryo kryo, Input input, Class<RTreeNode> type) {
        long numBytes = 0;
        boolean isLeaf = input.readBoolean();
        numBytes += 1;

        if(!isLeaf){
            RTreeNode node = null;
            // Read NonLeafNode
            MBR mbr = kryo.readObject(input, MBR.class);
            numBytes += mbr.getNumBytes();
            int numDimension = input.readInt();

            int numChildNodes = input.readInt();
            numBytes += 8;
            List<RTreeNode> childNodes = new ArrayList<RTreeNode>(numChildNodes);
            for(int i = 0; i< numChildNodes; i++){
                RTreeNode child = kryo.readObject(input, RTreeNode.class);
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
            MBR mbr = kryo.readObject(input, MBR.class);
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
                    Point point = kryo.readObject(input, Point.class);
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
                    PartitionedMBR partitionedMBR = kryo.readObject(input, PartitionedMBR.class);
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
