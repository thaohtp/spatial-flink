package flink;

import flink.datatype.RTreeNode;

/**
 * Created by JML on 3/7/17.
 */
public class RTree {

    private RTreeNode rootNode;

    public RTree(RTreeNode rootNode){
        this.rootNode = rootNode;
    }

    @Override
    public String toString(){
        return this.rootNode.toString();
    }

    // TODO: add insert, delete, update action
}
