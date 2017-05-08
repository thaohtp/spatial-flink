package flink.datatype;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 4/24/17.
 */
public class GlobalLeafNode extends RTreeNode implements Serializable{

    private List<PartitionPoint> partitionPointList;

    public GlobalLeafNode(int nbDimension){
        super(nbDimension, true);
        this.partitionPointList = new ArrayList<PartitionPoint>();
    }

    //TODO: For leaf node, what should be the pointer
    // Consider then add another list instead of List of Node
    public GlobalLeafNode(int nbDimension, MBR mbr, List<PartitionPoint> childNodes){
        super(nbDimension, mbr, true);
        this.partitionPointList = childNodes;
    }

    public void addPoint(PartitionPoint point){
        this.mbr.addMBR(point.getMbr());
        this.partitionPointList.add(point);
    }

    public String toString(){
        StringBuilder str = new StringBuilder("");
        for(int i =0; i< this.partitionPointList.size(); i++){
            PartitionPoint point = this.partitionPointList.get(i);
            str.append(point.toString());
        }
        return str.toString();
    }

    public List<PartitionPoint> getPartitionPointList() {
        return partitionPointList;
    }

    public void setPartitionPointList(List<PartitionPoint> partitionPointList) {
        this.partitionPointList = partitionPointList;
    }
}
