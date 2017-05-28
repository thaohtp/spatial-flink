package flink.datatype;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Key;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 3/7/17.
 */
public class Point implements Key<Point>{
    private List<Float> values;

    public Point(){
        this.values = new ArrayList<Float>();
    }

    public Point(List<Float> values){
        this.values = values;
    }

    public Point(float[] values){
        this.values = new ArrayList<Float>(values.length);
        for(int i =0; i<values.length; i++){
            this.values.add(values[i]);
        }
    }

    public Point add(Point point) {
        for(int i =0; i< this.values.size(); i++){
            float currentVal = this.values.get(i);
            float addedVal = point.getDimension(i);
            this.values.set(i, currentVal + addedVal);
        }
        return this;
    }

    public Point subtract(Point point) {
        for(int i =0; i< this.values.size(); i++){
            float currentVal = this.values.get(i);
            float addedVal = point.getDimension(i);
            this.values.set(i, currentVal - addedVal);
        }
        return this;
    }

    public Point divide(float division){
        for(int i =0; i< this.values.size(); i++){
            float currentVal = this.values.get(i);
            currentVal = currentVal / division;
            this.values.set(i, currentVal);
        }
        return this;
    }

    public int getNbDimension() {
        return this.values.size();
    }

    public float getDimension(int index) {
        return this.values.get(index);
    }

    public void setDimension(float value, int index) {
        this.values.set(index, value);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder("");
        str.append("(");
        for(int i =0; i <this.values.size() - 1; i++){
            str.append(this.values.get(i) + ",");
        }
        str.append(this.values.get(this.values.size()-1) + ")");
        return str.toString();
    }

    public int compare(Point point, int dimension) {
        float val1 = this.getDimension(dimension);
        float val2 = point.getDimension(dimension);
        return val1 > val2 ? 1 : (val1 == val2 ? 0 : -1);

    }

    @Override
    public boolean equals(Object obj) {
        Point point2 = (Point) obj;
        if(point2.getNbDimension() != this.getNbDimension()){
            return false;
        }
        for(int i =0; i<point2.getNbDimension(); i++){
            if(point2.getDimension(i) != this.getDimension(i)){
                return false;
            }
        }
        return true;
    }

    /**
     * Compares this object with the specified object for order.  Returns a
     * negative integer, zero, or a positive integer as this object is less
     * than, equal to, or greater than the specified object.
     * <p>
     * <p>The implementor must ensure <tt>sgn(x.compareTo(y)) ==
     * -sgn(y.compareTo(x))</tt> for all <tt>x</tt> and <tt>y</tt>.  (This
     * implies that <tt>x.compareTo(y)</tt> must throw an exception iff
     * <tt>y.compareTo(x)</tt> throws an exception.)
     * <p>
     * <p>The implementor must also ensure that the relation is transitive:
     * <tt>(x.compareTo(y)&gt;0 &amp;&amp; y.compareTo(z)&gt;0)</tt> implies
     * <tt>x.compareTo(z)&gt;0</tt>.
     * <p>
     * <p>Finally, the implementor must ensure that <tt>x.compareTo(y)==0</tt>
     * implies that <tt>sgn(x.compareTo(z)) == sgn(y.compareTo(z))</tt>, for
     * all <tt>z</tt>.
     * <p>
     * <p>It is strongly recommended, but <i>not</i> strictly required that
     * <tt>(x.compareTo(y)==0) == (x.equals(y))</tt>.  Generally speaking, any
     * class that implements the <tt>Comparable</tt> interface and violates
     * this condition should clearly indicate this fact.  The recommended
     * language is "Note: this class has a natural ordering that is
     * inconsistent with equals."
     * <p>
     * <p>In the foregoing description, the notation
     * <tt>sgn(</tt><i>expression</i><tt>)</tt> designates the mathematical
     * <i>signum</i> function, which is defined to return one of <tt>-1</tt>,
     * <tt>0</tt>, or <tt>1</tt> according to whether the value of
     * <i>expression</i> is negative, zero or positive.
     *
     * @param o the object to be compared.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     * @throws NullPointerException if the specified object is null
     * @throws ClassCastException   if the specified object's type prevents it
     *                              from being compared to this object.
     */
    @Override
    public int compareTo(Point o) {
        return 0;
    }

    @Override
    public void write(DataOutputView dataOutputView) throws IOException {
        for(int i = 0; i< this.values.size(); i++){
            dataOutputView.writeFloat(this.values.get(i));
        }
    }

    @Override
    public void read(DataInputView dataInputView) throws IOException {
        Float value = dataInputView.readFloat();
//        do{
//            value  = dataInputView.readFloat();
//            if(value != null){
//                this.values.add(value);
//            }
//        } while (value!= null);

//        value  = dataInputView.readFloat();
//        if(value != null){
//            this.values.add(value);
//        }
//        while(value != null){
//            this.values.add(value);
//            value = dataInputView.readFloat();
//        }
        this.values.add(value);
        value = dataInputView.readFloat();
        this.values.add(value);
    }

    public List<Float> getValues() {
        return values;
    }
}
