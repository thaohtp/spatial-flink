package flink.datatype;

import flink.RTree;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 3/31/17.
 */
public class NonLeafNode extends RTreeNode implements Serializable{

    private List<RTreeNode> childNodes;

    public NonLeafNode(){

    }

    public NonLeafNode(int nbDimension){
        super(nbDimension, false);
        this.childNodes = new ArrayList<RTreeNode>();
    }

    //For leaf node, what should be the pointer
    public NonLeafNode(int nbDimension, MBR mbr, List<RTreeNode> childNodes){
        super(nbDimension, mbr,false);
        this.childNodes = childNodes;
    }

    public void addChildNode(RTreeNode childNode){
        this.childNodes.add(childNode);
        this.mbr.addMBR(childNode.getMbr());
    }

    @Override
    public List<RTreeNode> getChildNodes() {
        return childNodes;
    }

    public void setChildNodes(List<RTreeNode> childNodes) {
        this.childNodes = childNodes;
    }

    public String toString(){
        StringBuilder str = new StringBuilder("");
        str.append("< ");
        for(int i = 0; i< this.childNodes.size(); i++){
            RTreeNode node = this.childNodes.get(i);
            str.append(node.toString() + " , ");
        }
        str.append(" > \n");
        return str.toString();
    }
}
