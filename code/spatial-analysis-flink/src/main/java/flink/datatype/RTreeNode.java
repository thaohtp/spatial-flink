package flink.datatype;

import java.io.Serializable;
import java.util.List;

/**
 * Created by JML on 3/31/17.
 */
public class RTreeNode implements Serializable {
    protected MBR mbr;
    protected boolean isLeaf = false;
    protected int nbDimension;

    protected long size;

    public RTreeNode(){
        this.size = 0;
    }

    public RTreeNode(int nbDimension, boolean isLeaf){
        this.nbDimension = nbDimension;
        this.isLeaf = isLeaf;
        this.mbr = new MBR(nbDimension);
        this.size = 0;
    }

    public RTreeNode(int nbDimension, MBR mbr, boolean isLeaf){
        this.nbDimension = nbDimension;
        this.mbr = mbr;
        this.isLeaf = isLeaf;
        this.size = 0;
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
        if(this instanceof PointLeafNode){
            PointLeafNode castedNode = (PointLeafNode) this;
            castedNode.addPoint(childPoint);
        }
        else{
            throw new Exception("Current node is not a leaf node");
        }

    }

    public void insert(PartitionedMBR childPoint) throws Exception{
        this.mbr.addMBR(childPoint.getMbr());
        if(this instanceof MBRLeafNode){
            MBRLeafNode castedNode = (MBRLeafNode) this;
            castedNode.addPoint(childPoint);
        }
        else{
            throw new Exception("Current node is not a global leaf node");
        }

    }

    public List<RTreeNode> getChildNodes(){
      return null;
    }

    public int getNbDimension() {
        return nbDimension;
    }

    public void setNbDimension(int nbDimension) {
        this.nbDimension = nbDimension;
    }

    /**
     * Get number of points
     * @return
     */
    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

}
