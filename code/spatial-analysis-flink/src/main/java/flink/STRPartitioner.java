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

        // TODO: search the point and return the partition number
//        pointString = pointString.substring(1, pointString.length()-2);
//        String[] floatStr = pointString.split(",");
//        List<Float> values = new ArrayList<Float>();
//        for(int k =0; k<floatStr.length; k++){
//            Float f = Float.parseFloat(floatStr[k]);
//            values.add(f);
//        }
//        Point point = new Point(values);
        // TODO: search the point and return the partition number
//        System.out.println("Out " + rTree.toString());
        List<RTreeNode> rTreeNodes = this.rTree.search(point);
        if(rTreeNodes != null && !rTreeNodes.isEmpty()){
            for(int k =0; k<rTreeNodes.size(); k++){
                MBRLeafNode MBRLeafNode = (MBRLeafNode) rTreeNodes.get(k);
                List<PartitionedMBR> partitions = MBRLeafNode.getPartitionedMBRList();
                for(int j = 0; j<partitions.size(); j++){
                    PartitionedMBR pPoint = partitions.get(j);
                    if(pPoint.getMbr().contains(point)){
                        System.out.println("MBR: " + pPoint.getMbr());
                        // TODO: be careful with this i
                        System.out.println("Partition number: " + pPoint.getPartitionNumber());
                        return pPoint.getPartitionNumber() % i;
                    }
                }
            }
        }

        rTreeNodes = this.rTree.getLeafNodes();
        PartitionedMBR chosenPPoint = null;
        double minDistance = Double.MAX_VALUE;
        for(int k =0; k<rTreeNodes.size(); k++){
            MBRLeafNode MBRLeafNode = (MBRLeafNode) rTreeNodes.get(k);
            List<PartitionedMBR> partitions = MBRLeafNode.getPartitionedMBRList();
            for(int j = 0; j<partitions.size(); j++){
                PartitionedMBR pPoint = partitions.get(j);
                double distance = pPoint.getMbr().calculateDistance(point);
                if(distance < minDistance){
                    chosenPPoint = pPoint;
                    minDistance = distance;
                }
            }
        }

        // update MBR of Partition Point
        chosenPPoint.getMbr().addPoint(point);
        return chosenPPoint.getPartitionNumber();
    }
}
