package flink.datatype;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 4/24/17.
 */
public class MBRLeafNode extends RTreeNode implements Serializable{

    private List<PartitionedMBR> partitionedMBRList;

    public MBRLeafNode(int nbDimension){
        super(nbDimension, true);
        this.partitionedMBRList = new ArrayList<PartitionedMBR>();
    }

    //TODO: For leaf node, what should be the pointer
    // Consider then add another list instead of List of Node
    public MBRLeafNode(int nbDimension, MBR mbr, List<PartitionedMBR> childNodes){
        super(nbDimension, mbr, true);
        this.partitionedMBRList = childNodes;
    }

    public void addPoint(PartitionedMBR point){
        this.mbr.addMBR(point.getMbr());
        this.partitionedMBRList.add(point);
    }

    public String toString(){
        StringBuilder str = new StringBuilder("");
        for(int i = 0; i< this.partitionedMBRList.size(); i++){
            PartitionedMBR point = this.partitionedMBRList.get(i);
            str.append(point.toString());
        }
        return str.toString();
    }

    public List<PartitionedMBR> getPartitionedMBRList() {
        return partitionedMBRList;
    }

    public void setPartitionedMBRList(List<PartitionedMBR> partitionedMBRList) {
        this.partitionedMBRList = partitionedMBRList;
    }
}
