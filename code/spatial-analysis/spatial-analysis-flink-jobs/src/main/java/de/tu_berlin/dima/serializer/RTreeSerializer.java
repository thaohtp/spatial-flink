package de.tu_berlin.dima.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.datatype.NonLeafNode;

/**
 * Created by JML on 6/28/17.
 */
public class RTreeSerializer extends Serializer<RTree>{
    @Override
    public void write(Kryo kryo, Output output, RTree object) {
        kryo.writeObject(output,object.getRootNode());
    }

    @Override
    public RTree read(Kryo kryo, Input input, Class<RTree> type) {
        NonLeafNode root = kryo.readObject(input, NonLeafNode.class);
        RTree tree = new RTree(root);
        tree.setNumBytes(root.getNumBytes());
        return tree;
    }
}
