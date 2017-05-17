package flink.datatype;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 3/31/17.
 */
public class PointLeafNode extends RTreeNode{

    private List<Point> pointList;

    public PointLeafNode(int nbDimension){
        super(nbDimension, true);
        this.pointList = new ArrayList<Point>();
    }

    //TODO: For leaf node, what should be the pointer
    // Consider then add another list instead of List of Node
    public PointLeafNode(int nbDimension, MBR mbr, List<Point> childNodes){
        super(nbDimension, mbr, true);
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

    @Override
    public boolean equals(Object obj) {
        PointLeafNode leaf2 = (PointLeafNode) obj;
        if(!leaf2.getPointList().containsAll(this.getPointList())){
            return false;
        }
        if(!this.getPointList().containsAll(leaf2.getPointList())){
            return false;
        }
        if(!leaf2.getMbr().equals(this.getMbr())){
            return false;
        }

        return true;
    }
}
