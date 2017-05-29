package flink.datatype;

import java.io.Serializable;

/**
 * Created by JML on 4/24/17.
 */
public class MBRLeafNode extends LeafNode<PartitionedMBR> implements Serializable{
    public MBRLeafNode(int nbDimension) {
        super(nbDimension);
    }

}
