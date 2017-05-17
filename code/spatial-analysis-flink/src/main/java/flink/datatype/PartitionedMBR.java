package flink.datatype;

import java.io.Serializable;

/**
 * Created by JML on 4/25/17.
 */
public class PartitionedMBR implements Serializable{
    private MBR mbr;

    private int partitionNumber;
    public PartitionedMBR(MBR mbr, Integer partitionNumber){
        this.mbr = mbr;
        this.partitionNumber = partitionNumber;
    }

    public MBR getMbr() {
        return mbr;
    }

    public void setMbr(MBR mbr) {
        this.mbr = mbr;
    }

    public int getPartitionNumber() {
        return partitionNumber;
    }

    public void setPartitionNumber(int partitionNumber) {
        this.partitionNumber = partitionNumber;
    }

    public String toString(){
        return Integer.toString(this.partitionNumber);
    }

}
