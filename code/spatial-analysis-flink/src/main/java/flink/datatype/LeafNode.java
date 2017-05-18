package flink.datatype;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 5/18/17.
 */
public abstract class LeafNode<T> extends RTreeNode implements Serializable{

    protected List<T> entries;

    public LeafNode(int nbDimension){
        super(nbDimension, true);
        this.entries = new ArrayList<T>();
    }

    public LeafNode(int nbDimension, MBR mbr, List<T> entries){
        super(nbDimension, true);
        this.entries = entries;
    }

    public void addPoint(T entry){
        if(entry instanceof Point){
            Point point = (Point) entry;
            this.mbr.addPoint(point);
        }
        else{
            if(entry instanceof PartitionedMBR){
                PartitionedMBR mbr = (PartitionedMBR) entry;
                this.mbr.addMBR(mbr.getMbr());
            }
        }
        this.entries.add(entry);
    }

    public String toString(){
        StringBuilder str = new StringBuilder("");
        for(int i =0; i<this.entries.size(); i++){
            T entry = this.entries.get(i);
            str.append(entry.toString());
        }
        return str.toString();
    }

    public List<T> getEntries() {
        return entries;
    }

    public void setEntries(List<T> entries) {
        this.entries = entries;
    }
}
