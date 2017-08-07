package de.tu_berlin.dima.datatype;

import java.io.Serializable;

/**
 * Created by JML on 8/2/17.
 */
public class JoinedRow implements Serializable{
    private Point left;
    private Point right;

    public JoinedRow(Point left, Point right){
        this.left = left;
        this.right = right;
    }

    public Point getLeft() {
        return left;
    }

    public void setLeft(Point left) {
        this.left = left;
    }

    public Point getRight() {
        return right;
    }

    public void setRight(Point right) {
        this.right = right;
    }

    public String toString(){
        return this.left.toString() + " - " + this.right.toString();
    }
}
