package flink;

import flink.datatype.Point;
import flink.datatype.RTreeNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 3/7/17.
 */
public class RTree implements Serializable{

    private RTreeNode rootNode;

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
        List<RTreeNode> result = this.searchNodes(rootNode, point);
        return result;
    }

    private List<RTreeNode> searchNodes(RTreeNode node, Point point){
        List<RTreeNode> result = new ArrayList<RTreeNode>();
        if(node.getMbr().contains(point)){
            if(node.isLeaf()){
                result.add(node);
                return result;
            }
            else{
                List<RTreeNode> childNodes = node.getChildNodes();
                for(int i =0; i<childNodes.size(); i++){
                    result.addAll(searchNodes(childNodes.get(i), point));
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

}
