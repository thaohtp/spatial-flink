package de.tu_berlin.dima;

import de.tu_berlin.dima.datatype.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 3/7/17.
 */
public class RTree implements Serializable{

    private RTreeNode rootNode;

    private long numBytes;

    public RTree(RTreeNode rootNode){
        this.rootNode = rootNode;
    }

    public RTreeNode getRootNode() {
        return rootNode;
    }

    public void setRootNode(RTreeNode rootNode) {
        this.rootNode = rootNode;
    }

    @Override
    public String toString(){
        return this.rootNode.toString();
    }

    // TODO: add insert, delete, update action

    // TODO: insert point
    // The behavior is defined in Gut84.pdf file (the original paper of R-Tree)
    public void insert(Point point){
        //perform three actions
        // 1. choose leaf to place the point
        // 2. split node if there is no place in the node
        // 3. adjust tree if we need to grow tree or create a new root node
    }


    private void chooseLeaf(){

    }

    private void splitNode(){

    }

    private void adjustTree(){

    }

    // TODO: should we implement delete action now
    public void delete(Point point){

    }

    public List<RTreeNode> search(Point point){
        return this.searchByPoint(rootNode, point);
    }

    public List<PartitionedMBR> search(MBR mbr){
        return this.searchByRectangle(rootNode, mbr);
    }

    private List<RTreeNode> searchByPoint(RTreeNode node, Point point){
        List<RTreeNode> result = new ArrayList<RTreeNode>();
        if(node.getMbr().contains(point)){
            if(node.isLeaf()){
                result.add(node);
                return result;
            }
            else{
                List<RTreeNode> childNodes = node.getChildNodes();
                for(int i =0; i<childNodes.size(); i++){
                    result.addAll(searchByPoint(childNodes.get(i), point));
                }
            }
        }
        return result;
    }

    private List<PartitionedMBR> searchByRectangle(RTreeNode node, MBR box){
        List<PartitionedMBR> result = new ArrayList<PartitionedMBR>();
        if(node.getMbr().intersects(box)){
            if(node.isLeaf()){
                List<PartitionedMBR> entries = ((MBRLeafNode) node).getEntries();
                for (PartitionedMBR entry: entries) {
                    if(entry.getMbr().intersects(box)){
                        result.add(entry);
                    }
                }
                return result;
            }
            else{
                List<RTreeNode> childNodes = node.getChildNodes();
                for(int i =0; i<childNodes.size(); i++){
                    result.addAll(searchByRectangle(childNodes.get(i), box));
                }
            }
        }
        return result;
    }

    public List<RTreeNode> getLeafNodes(){
        return searchLeafNodes(this.rootNode);
    }

    private List<RTreeNode> searchLeafNodes(RTreeNode node){
        List<RTreeNode> result = new ArrayList<RTreeNode>();
        List<RTreeNode> childNodes = node.getChildNodes();
        if(childNodes.get(0).isLeaf()){
            result.addAll(node.getChildNodes());
            return result;
        }
        else{
            for(int i =0; i< childNodes.size(); i++){
                result.addAll(searchLeafNodes(childNodes.get(i)));
            }
        }
        return result;
    }

    public long getNumBytes() {
        return rootNode.getNumBytes();
//        return numBytes;
    }

    public void setNumBytes(long numBytes) {
        this.numBytes = numBytes;
    }
}
