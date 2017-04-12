package flink.datatype;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 3/31/17.
 */
public class LeafNode extends RTreeNode{

    private List<Point> pointList;

    public LeafNode(){
        super(true);
        this.pointList = new ArrayList<Point>();
    }

    //TODO: For leaf node, what should be the pointer
    // Consider then add another list instead of List of Node
    public LeafNode(MBR mbr, List<Point> childNodes){
        super(mbr, true);
        this.pointList = childNodes;
    }

    public void addPoint(Point point){
        this.mbr.addPoint(point);
        this.pointList.add(point);
    }

    public List<Point> getPointList() {
        return pointList;
    }

    public void setPointList(List<Point> pointList) {
        this.pointList = pointList;
    }

    public String toString(){
        StringBuilder str = new StringBuilder("");
        for(int i =0; i< this.getPointList().size(); i++){
            Point point = this.getPointList().get(i);
            str.append(point.toString());
        }
        return str.toString();
    }
}
