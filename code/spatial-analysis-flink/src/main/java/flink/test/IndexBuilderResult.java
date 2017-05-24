package flink.test;

import flink.RTree;
import flink.STRPartitioner;
import org.apache.flink.api.java.DataSet;

import java.util.List;

/**
 * Created by JML on 5/19/17.
 */
public class IndexBuilderResult {
    private RTree globalRTree;
    private DataSet<RTree> localRTree;
    private STRPartitioner strPartitioner;

    public IndexBuilderResult(RTree globalRTree, DataSet<RTree> localRTree, STRPartitioner strPartitioner) {
        this.globalRTree = globalRTree;
        this.localRTree = localRTree;
        this.strPartitioner = strPartitioner;
    }

    public List<RTree> getLocalRTreeArray() throws Exception {
        return localRTree.collect();
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

    public STRPartitioner getStrPartitioner() {
        return strPartitioner;
    }

    public void setStrPartitioner(STRPartitioner strPartitioner) {
        this.strPartitioner = strPartitioner;
    }
}
