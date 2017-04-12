package flink.datatype;

/**
 * Created by JML on 3/31/17.
 */
public class MBR {

    // Point1, point2 define rectangle
    private Point maxPoint;
    private Point minPoint;
    private boolean isInitialized = true;

    public MBR(){
        this.maxPoint = new Point(Float.MAX_VALUE, Float.MAX_VALUE);
        this.minPoint = new Point(Float.MIN_VALUE, Float.MIN_VALUE);
    }

    public MBR(Point point1, Point point2){
        this.maxPoint = new Point(Float.MAX_VALUE, Float.MAX_VALUE);
        this.minPoint = new Point(Float.MIN_VALUE, Float.MIN_VALUE);

        this.addPoint(point1);
        this.addPoint(point2);

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
        if(this.isInitialized){
            this.maxPoint.setX(point.getX());
            this.maxPoint.setY(point.getY());
            this.minPoint.setX(point.getX());
            this.minPoint.setY(point.getY());
//            this.minPoint = point;
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

    // TODO: how can we compare efficiently here
    public void addMBR(MBR mbr){
        if(this.contains(mbr)){
            return;
        }

        // TODO: how to compare here???
        // put inside or intersect, or above, or under

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
        return true;
    }

    public int compare(MBR mbr, int dimension){
        //TODO: is medium is the best way to compare two MBR?
        float val1 = (this.maxPoint.getDimension(dimension) + this.minPoint.getDimension(dimension)) /2;
        float val2 = (mbr.getMaxPoint().getDimension(dimension) + this.getMinPoint().getDimension(dimension)) /2;
        return val1 > val2 ? 1: (val1 == val2 ? 0: -1);

    }
}
