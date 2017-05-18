package flink.datatype;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 3/31/17.
 */
public class PointLeafNode extends LeafNode<Point>{

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
}
