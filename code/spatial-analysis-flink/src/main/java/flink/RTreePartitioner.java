package flink;

import flink.datatype.GlobalLeafNode;
import flink.datatype.PartitionPoint;
import flink.datatype.Point;
import flink.datatype.RTreeNode;
import org.apache.avro.generic.GenericData;
import org.apache.commons.math3.stat.inference.TestUtils;
import org.apache.flink.api.common.functions.Partitioner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Created by JML on 5/7/17.
 */
public class RTreePartitioner implements Partitioner<String> {

    private RTree rTree;

    public RTreePartitioner(RTree rTree){
        this.rTree = rTree;
        System.out.println("Test rTRee: " + rTree.getRootNode().getMbr().toString());
    }

    @Override
    public int partition(String pointString, int i) {
        // TODO: should we insert point and how to update it

        // TODO: search the point and return the partition number
//        List<RTreeNode> rTreeNodes = this.rTree.search(point);
//        for(int k =0; k<rTreeNodes.size(); k++){
//            GlobalLeafNode globalLeafNode = (GlobalLeafNode) rTreeNodes.get(k);
//            List<PartitionPoint> partitions = globalLeafNode.getPartitionPointList();
//            for(int j = 0; j<partitions.size(); i++){
//                PartitionPoint pPoint = partitions.get(j);
//                if(pPoint.getMbr().contains(point)){
//                    // TODO: be careful with this i
//                    return pPoint.getPartitionNumber() % i;
//                }
//            }
//        }
        pointString = pointString.substring(1, pointString.length()-2);
        String[] floatStr = pointString.split(",");
        List<Float> values = new ArrayList<Float>();
        for(int k =0; k<floatStr.length; k++){
            Float f = Float.parseFloat(floatStr[k]);
            values.add(f);
        }
        Point point = new Point(values);
        // TODO: search the point and return the partition number
//        System.out.println("Out " + rTree.toString());
        System.out.println("Test rTRee:2 " + rTree.getRootNode().getMbr().toString());
        List<RTreeNode> rTreeNodes = this.rTree.search(point);
        for(int k =0; k<rTreeNodes.size(); k++){
            GlobalLeafNode globalLeafNode = (GlobalLeafNode) rTreeNodes.get(k);
            List<PartitionPoint> partitions = globalLeafNode.getPartitionPointList();
            for(int j = 0; j<partitions.size(); i++){
                PartitionPoint pPoint = partitions.get(j);
                if(pPoint.getMbr().contains(point)){
                    // TODO: be careful with this i
                    System.out.println("Partition number: " + pPoint.getPartitionNumber());
                    return pPoint.getPartitionNumber() % i;
                }
            }
        }

        return 0;
    }
}
