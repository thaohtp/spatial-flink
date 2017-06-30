package de.tu_berlin.dima;

import de.tu_berlin.dima.datatype.MBRLeafNode;
import de.tu_berlin.dima.datatype.PartitionedMBR;
import de.tu_berlin.dima.datatype.Point;
import de.tu_berlin.dima.datatype.RTreeNode;
import org.apache.flink.api.common.functions.Partitioner;

import java.util.List;

/**
 * Created by JML on 5/7/17.
 */
public class STRPartitioner implements Partitioner<Point> {

    private RTree rTree;

    public STRPartitioner(RTree rTree){
        this.rTree = rTree;
    }

    @Override
    public int partition(Point point, int i) {
        List<RTreeNode> rTreeNodes = this.rTree.search(point);
        // If we could not find MBR which contains the Point, then get all leaf nodes and calculate the distance
        if(rTreeNodes == null || rTreeNodes.isEmpty()){
            rTreeNodes = this.rTree.getLeafNodes();
        }

        PartitionedMBR chosenPPoint = null;
        double minDistance = Double.MAX_VALUE;
        for(int k =0; k<rTreeNodes.size(); k++){
            MBRLeafNode mbrLeafNode = (MBRLeafNode) rTreeNodes.get(k);
            List<PartitionedMBR> partitions = mbrLeafNode.getEntries();
            for(int j = 0; j<partitions.size(); j++){
                PartitionedMBR mbr = partitions.get(j);
                double distance = mbr.getMbr().calculateDistance(point);
                if(distance < minDistance){
                    chosenPPoint = mbr;
                    minDistance = distance;
                }
            }
        }

        return chosenPPoint.getPartitionNumber();
    }

    public RTree getrTree() {
        return rTree;
    }

    public void setrTree(RTree rTree) {
        this.rTree = rTree;
    }
}
