package flink;

import flink.datatype.MBRLeafNode;
import flink.datatype.PartitionedMBR;
import flink.datatype.Point;
import flink.datatype.RTreeNode;
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
        // TODO: should we insert point and how to update it
        List<RTreeNode> rTreeNodes = this.rTree.search(point);
        // If we could not find MBR which contains the Point, then get all leaf nodes and calculate the distance
        if(rTreeNodes == null || rTreeNodes.isEmpty()){
            System.out.println("Not matched");
            rTreeNodes = this.rTree.getLeafNodes();
        }
//        for(int k =0; k<rTreeNodes.size(); k++){
//            MBRLeafNode MBRLeafNode = (MBRLeafNode) rTreeNodes.get(k);
//            List<PartitionedMBR> partitions = MBRLeafNode.getPartitionedMBRList();
//            for(int j = 0; j<partitions.size(); j++){
//                PartitionedMBR mbr = partitions.get(j);
//                if(mbr.getMbr().contains(point)){
//                    System.out.println("MBR: " + mbr.getMbr());
//                    // TODO: be careful with this i
//                    System.out.println("Partition number: " + mbr.getPartitionNumber());
//                    return mbr.getPartitionNumber() % i;
//                }
//            }
//        }

//        rTreeNodes = this.rTree.getLeafNodes();
        PartitionedMBR chosenPPoint = null;
        double minDistance = Double.MAX_VALUE;
        for(int k =0; k<rTreeNodes.size(); k++){
            MBRLeafNode MBRLeafNode = (MBRLeafNode) rTreeNodes.get(k);
            List<PartitionedMBR> partitions = MBRLeafNode.getPartitionedMBRList();
            for(int j = 0; j<partitions.size(); j++){
                PartitionedMBR mbr = partitions.get(j);
                double distance = mbr.getMbr().calculateDistance(point);
                if(distance < minDistance){
                    chosenPPoint = mbr;
                    minDistance = distance;
                }
            }
        }

        // update MBR of Partition Point
        chosenPPoint.getMbr().addPoint(point);
        return chosenPPoint.getPartitionNumber();
    }
}
