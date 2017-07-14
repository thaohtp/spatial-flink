package de.tu_berlin.dima.datatype;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 3/31/17.
 */
public class MBR implements Serializable{

    // Point1, point2 define rectangle
    private Point maxPoint;
    private Point minPoint;
    private boolean isInitialized = true;
    private int nbDimension;
    private long size;
    private int numBytes;

    public MBR(int nbDimension){
        this.nbDimension = nbDimension;
        List<Float> maxList = new ArrayList<Float>(nbDimension);
        List<Float> minList = new ArrayList<Float>(nbDimension);
        for(int i =0; i<nbDimension; i++){
            maxList.add(i, Float.MAX_VALUE);
            minList.add(i, Float.MIN_VALUE);
        }
        this.maxPoint = new Point(maxList);
        this.minPoint = new Point(minList);
    }

    public MBR(Point minPoint, Point maxPoint){
        this.nbDimension = maxPoint.getNbDimension();
        this.maxPoint = maxPoint;
        this.minPoint = minPoint;
        this.isInitialized = false;

    }

    public Point getMaxPoint() {
        return maxPoint;
    }

    public void setMaxPoint(Point maxPoint) {
        this.maxPoint = maxPoint;
    }

    public Point getMinPoint() {
        return minPoint;
    }

    public void setMinPoint(Point minPoint) {
        this.minPoint = minPoint;
    }

    // add methods to update point whenever adding a new point

    public void addPoint(Point point){
        // TODO: inefficient check here
        if(this.isInitialized){
            for(int i =0; i<nbDimension; i++){
                this.maxPoint.setDimension(point.getDimension(i), i);
                this.minPoint.setDimension(point.getDimension(i), i);
            }
            this.isInitialized = false;
        }
        if(this.contains(point)){
            return;
        }
        // TODO: check and update MBR when inserting a new point
        for(int i = 0; i< point.getNbDimension(); i++){
            float pointVal = point.getDimension(i);
            if(this.getMaxPoint().getDimension(i) < pointVal){
                this.getMaxPoint().setDimension(pointVal, i);
            }

            if(this.getMinPoint().getDimension(i) > pointVal){
                this.getMinPoint().setDimension(pointVal, i);
            }
        }
    }

    public void addPoints(List<Point> points){
        for (Point point: points) {
            this.addPoint(point);
        }
    }

    public void addMBR(MBR mbr){
        this.addPoint(mbr.getMaxPoint());
        this.addPoint(mbr.getMinPoint());

    }

    public boolean contains(Point point){
        for(int i = 0; i< point.getNbDimension(); i++){
            float pointVal = point.getDimension(i);
            float maxVal = this.maxPoint.getDimension(i);
            float minVal = this.minPoint.getDimension(i);
            if(pointVal > maxVal || pointVal < minVal){
                return false;
            }
        }
        return true;
    }

    //TODO: check if MBR is contain in this MBR or not
    public boolean contains(MBR mbr){
        for(int i = 0; i< mbr.getNbDimension(); i++){
            float pointMaxVal = mbr.getMaxPoint().getDimension(i);
            float pointMinVal = mbr.getMinPoint().getDimension(i);
            float maxVal = this.maxPoint.getDimension(i);
            float minVal = this.minPoint.getDimension(i);
            if(pointMaxVal > maxVal || pointMaxVal < minVal || pointMinVal > maxVal || pointMinVal < minVal){
                return false;
            }
        }
        return true;
    }

    public int compare(MBR mbr, int dimension){
        //TODO: is medium is the best way to compare two MBR?
//        System.out.println("MBR compare: " + this.toString() + " - " + mbr.toString() + " - " + dimension);
        double val1 = (this.maxPoint.getDimension(dimension) + this.minPoint.getDimension(dimension)) /2.0;
        double val2 = (mbr.getMaxPoint().getDimension(dimension) + mbr.getMinPoint().getDimension(dimension)) /2.0;
        return val1 > val2 ? 1: (val1 == val2 ? 0: -1);
    }

    @Override
    public boolean equals(Object obj) {
        MBR mbr2 = (MBR) obj;
        if(!mbr2.getMinPoint().equals(this.getMinPoint()) || !mbr2.getMaxPoint().equals(this.getMaxPoint())){
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "MBR: Min" + this.minPoint.toString() + " - Max" + this.getMaxPoint().toString();
    }

    public double calculateDistance(Point point){
        double distance = 0L;
        for(int i =0; i< nbDimension; i++){
            float center = (this.maxPoint.getDimension(i) + this.minPoint.getDimension(i)) /2;
            float distanceL1 = center - point.getDimension(i);
            distance = distance + Math.pow(distanceL1, 2);
        }
        return Math.sqrt(distance);
    }

    public boolean intersects(MBR mbr){
        for(int i =0; i< this.getNbDimension(); i++){
            if(this.getMaxPoint().getDimension(i) < mbr.getMinPoint().getDimension(i) || this.getMinPoint().getDimension(i) > mbr.getMaxPoint().getDimension(i)){
                return false;
            }
        }
        return true;
    }

    public boolean isInitialized() {
        return isInitialized;
    }
    public void setInitialized(boolean initialized) {
        isInitialized = initialized;
    }

    public int getNbDimension() {
        return nbDimension;
    }

    public void setNbDimension(int nbDimension) {
        this.nbDimension = nbDimension;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public int getNumBytes() {
        return maxPoint.getNumbBytes() + minPoint.getNumbBytes() + 1 + 4;
//        return numBytes;
    }

    public void setNumBytes(int numBytes) {
        this.numBytes = numBytes;
    }
}
