package de.tu_berlin.dima.datatype;

import java.io.Serializable;

/**
 * Created by JML on 3/31/17.
 */
public class PointLeafNode extends LeafNode<Point> implements Serializable{

    public PointLeafNode(int nbDimension){
        super(nbDimension);
    }

    @Override
    public boolean equals(Object obj) {
        PointLeafNode leaf2 = (PointLeafNode) obj;
        if(!leaf2.getEntries().containsAll(this.getEntries())){
            return false;
        }
        if(!this.getEntries().containsAll(leaf2.getEntries())){
            return false;
        }
        if(!leaf2.getMbr().equals(this.getMbr())){
            return false;
        }

        return true;
    }

    @Override
    public long getNumBytes() {
        this.numBytes = entries.size() * (4 * (nbDimension +1));
        return 1 + mbr.getNumBytes() + 8 + 1 + this.numBytes;
//        return super.getNumBytes();
    }

}
