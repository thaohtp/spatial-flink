package flink.datatype;

import flink.RTree;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 3/31/17.
 */
public class RTreeNode {
    protected MBR mbr;
    protected boolean isLeaf = false;

    public RTreeNode(boolean isLeaf){
        this.isLeaf = isLeaf;
        this.mbr = new MBR();
    }

    public RTreeNode(MBR mbr, boolean isLeaf){
        this.mbr = mbr;
        this.isLeaf = isLeaf;
    }

    public MBR getMbr() {
        return mbr;
    }

    public void setMbr(MBR mbr) {
        this.mbr = mbr;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    // TODO: check again insert here if this is OK or not
    public void insert(RTreeNode childNode) throws Exception{
        this.mbr.addMBR(childNode.getMbr());
        if(this instanceof NonLeafNode){
            NonLeafNode castedNode = (NonLeafNode) this;
            castedNode.addChildNode(childNode);
        }
        else{
            throw new Exception("Current node is not a non-leaf node");
        }
    }

    /**
     * Returns a string representation of the object. In general, the
     * {@code toString} method returns a string that
     * "textually represents" this object. The result should
     * be a concise but informative representation that is easy for a
     * person to read.
     * It is recommended that all subclasses override this method.
     * <p>
     * The {@code toString} method for class {@code Object}
     * returns a string consisting of the name of the class of which the
     * object is an instance, the at-sign character `{@code @}', and
     * the unsigned hexadecimal representation of the hash code of the
     * object. In other words, this method returns a string equal to the
     * value of:
     * <blockquote>
     * <pre>
     * getClass().getName() + '@' + Integer.toHexString(hashCode())
     * </pre></blockquote>
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {
        return super.toString();
    }

    public void insert(Point childPoint) throws Exception{
        this.mbr.addPoint(childPoint);
        if(this instanceof LeafNode){
            LeafNode castedNode = (LeafNode) this;
            castedNode.addPoint(childPoint);
        }
        else{
            throw new Exception("Current node is not a leaf node");
        }

    }

    public List<RTreeNode> getChildNodes(){
      return null;
    }

}
