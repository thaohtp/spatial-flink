package flink.datatype;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 3/7/17.
 */
public class Point {
    private float x;
    private float y;
    private float z;

    private List<Float> values;


    public Point(float x, float y) {
        this.values = new ArrayList<Float>(2);
        this.x = x;
        this.y = y;
        this.values.add(this.x);
        this.values.add(this.y);
    }

    public Point(float x, float y, float z) {
        this.values = new ArrayList<Float>(3);
        this.x = x;
        this.y = y;
        this.z = z;

        this.values.add(this.x);
        this.values.add(this.y);
        this.values.add(this.z);
    }

    public float getX() {
        return x;
    }

    public void setX(float x) {
        this.x = x;
    }

    public float getY() {
        return y;
    }

    public void setY(float y) {
        this.y = y;
    }

    public Point add(Point point) {
        return new Point(this.x + point.getX(), this.y + point.getY());
    }

    public Point subtract(Point point) {
        return new Point(this.x - point.getX(), this.y - point.getY());
    }

    public int getNbDimension() {
        return this.values.size();
    }

    public float getDimension(int index) {
        if (index == 0) {
            return getX();
        }
        return getY();
    }

    public void setDimension(float value, int index) {
        if (index == 0) {
            this.setX(value);
        } else {
            if (index == 1) {
                this.setY(value);
            }
        }
    }

    @Override
    public String toString() {
        return "(" + getX() + "," + getY() + ")";
    }

    public int compare(Point point, int dimension) {
        float val1 = this.getDimension(dimension);
        float val2 = point.getDimension(dimension);
        return val1 > val2 ? 1 : (val1 == val2 ? 0 : -1);

    }


}
