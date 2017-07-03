package de.tu_berlin.dima.datatype;

import java.io.Serializable;

/**
 * Created by JML on 4/24/17.
 */
public class MBRLeafNode extends LeafNode<PartitionedMBR> implements Serializable{
    public MBRLeafNode(int nbDimension) {
        super(nbDimension);
    }

    @Override
    public long getNumBytes() {
        this.numBytes =0;
        for(int i =0; i< entries.size(); i++){
            this.numBytes += entries.get(i).getNumBytes();
        }
        return 1 + mbr.getNumBytes() + 8 + 1 + this.numBytes;
//        return super.getNumBytes();
    }

}
