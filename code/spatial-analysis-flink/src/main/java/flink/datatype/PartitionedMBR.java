package flink.datatype;

import java.io.Serializable;

/**
 * Created by JML on 4/25/17.
 */
public class PartitionedMBR implements Serializable{
    private MBR mbr;

    private long size;

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
        this.size = mbr.getSize();
    }

    public int getPartitionNumber() {
        return partitionNumber;
    }

    public void setPartitionNumber(int partitionNumber) {
        this.partitionNumber = partitionNumber;
    }

    public String toString(){
        return "[" + Integer.toString(this.partitionNumber) + ", " + this.mbr.toString() + " ]";
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

}
