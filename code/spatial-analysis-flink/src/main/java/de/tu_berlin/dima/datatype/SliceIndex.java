package de.tu_berlin.dima.datatype;

/**
 * Created by JML on 4/25/17.
 */
public class SliceIndex {
    private int startIndex;
    private int endIndex;

    public SliceIndex(int startIndex, int endIndex) {
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public int getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(int startIndex) {
        this.startIndex = startIndex;
    }

    public int getEndIndex() {
        return endIndex;
    }

    public void setEndIndex(int endIndex) {
        this.endIndex = endIndex;
    }

    public String toString() {
        return this.startIndex + " - " + this.endIndex;
    }
}
