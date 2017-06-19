package de.tu_berlin.dima.test;

import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.STRPartitioner;
import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.STRPartitioner;
import de.tu_berlin.dima.datatype.Point;
import org.apache.flink.api.java.DataSet;

/**
 * Created by JML on 5/19/17.
 */
public class IndexBuilderResult {
    private RTree globalRTree;
    private DataSet<RTree> localRTree;
    private STRPartitioner partitioner;
    private DataSet<Point> data;

    public IndexBuilderResult(DataSet<Point> data, RTree globalRTree, DataSet<RTree> localRTree, STRPartitioner strPartitioner) {
        this.data = data;
        this.globalRTree = globalRTree;
        this.localRTree = localRTree;
        this.partitioner = strPartitioner;
    }

    public RTree getGlobalRTree() {
        return globalRTree;
    }

    public void setGlobalRTree(RTree globalRTree) {
        this.globalRTree = globalRTree;
    }

    public DataSet<RTree> getLocalRTree() {
        return localRTree;
    }

    public void setLocalRTree(DataSet<RTree> localRTree) {
        this.localRTree = localRTree;
    }

    public STRPartitioner getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(STRPartitioner partitioner) {
        this.partitioner = partitioner;
    }

    public DataSet<Point> getData() {
        return data;
    }

    public void setData(DataSet<Point> data) {
        this.data = data;
    }
}
