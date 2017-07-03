package de.tu_berlin.dima.datatype;

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
        this.size = 0;
    }

    //For leaf node, what should be the pointer
    public NonLeafNode(int nbDimension, MBR mbr, List<RTreeNode> childNodes){
        super(nbDimension, mbr,false);
        this.childNodes = childNodes;
        this.size = childNodes.size();
    }

    public void addChildNode(RTreeNode childNode){
        this.childNodes.add(childNode);
        this.mbr.addMBR(childNode.getMbr());
        this.size += childNode.getSize();
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

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    @Override
    public long getNumBytes() {
        this.numBytes = 0;
        for(int i =0; i< childNodes.size(); i++){
            this.numBytes += childNodes.get(i).getNumBytes();
        }
        return 1 + mbr.getNumBytes() + 8 + this.numBytes;
//        return super.getNumBytes();
    }
}
