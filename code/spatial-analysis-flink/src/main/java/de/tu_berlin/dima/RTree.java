package de.tu_berlin.dima;

import de.tu_berlin.dima.datatype.*;

import java.io.Serializable;
import java.util.*;

/**
 * Created by JML on 3/7/17.
 */
public class RTree implements Serializable{

    private RTreeNode rootNode;

    private long numBytes;

    public RTree(RTreeNode rootNode){
        this.rootNode = rootNode;
    }

    public RTreeNode getRootNode() {
        return rootNode;
    }

    public void setRootNode(RTreeNode rootNode) {
        this.rootNode = rootNode;
    }

    @Override
    public String toString(){
        return this.rootNode.toString();
    }

    public List<RTreeNode> search(Point point){
        return this.searchByPoint(rootNode, point);
    }

    public List<PartitionedMBR> search(MBR mbr){
        return this.searchByRectangle(rootNode, mbr);
    }

    public List<PartitionedMBR> search(Point point, float radius){
        return this.searchByCircle(rootNode, point, radius);
    }

    private List<RTreeNode> searchByPoint(RTreeNode node, Point point){
        List<RTreeNode> result = new ArrayList<RTreeNode>();
        if(node.getMbr().contains(point)){
            if(node.isLeaf()){
                result.add(node);
                return result;
            }
            else{
                List<RTreeNode> childNodes = node.getChildNodes();
                for(int i =0; i<childNodes.size(); i++){
                    result.addAll(searchByPoint(childNodes.get(i), point));
                }
            }
        }
        return result;
    }

    private List<PartitionedMBR> searchByRectangle(RTreeNode node, MBR box){
        List<PartitionedMBR> result = new ArrayList<PartitionedMBR>();
        if(node.getMbr().intersects(box)){
            if(node.isLeaf()){
                List<PartitionedMBR> entries = ((MBRLeafNode) node).getEntries();
                for (PartitionedMBR entry: entries) {
                    if(entry.getMbr().intersects(box)){
                        result.add(entry);
                    }
                }
                return result;
            }
            else{
                List<RTreeNode> childNodes = node.getChildNodes();
                for(int i =0; i<childNodes.size(); i++){
                    result.addAll(searchByRectangle(childNodes.get(i), box));
                }
            }
        }
        return result;
    }

    public List<PartitionedMBR> searchByCircle(RTreeNode node, Point queryPoint, float radius){
        List<PartitionedMBR> result = new ArrayList<PartitionedMBR>();
        if(node.getMbr().intersects(queryPoint, radius)){
            if(node.isLeaf()){
                List<PartitionedMBR> entries = ((MBRLeafNode) node).getEntries();
                for (PartitionedMBR entry: entries) {
                    if(entry.getMbr().intersects(queryPoint, radius)){
                        result.add(entry);
                    }
                }
                return result;
            }
            else{
                List<RTreeNode> childNodes = node.getChildNodes();
                for(int i =0; i<childNodes.size(); i++){
                    result.addAll(searchByCircle(childNodes.get(i), queryPoint, radius));
                }
            }
        }
        return result;

    }

    public List<Point> circleRange(Point queryPoint, double radius){
        List<Point> result = new ArrayList<Point>();
        List<RTreeNode> nodes = new ArrayList<RTreeNode>();
        nodes.add(this.rootNode);
        while(!nodes.isEmpty()){
            RTreeNode curNode = nodes.remove(0);
            if(curNode.getMbr().intersects(queryPoint, radius)){
                if(curNode.isLeaf()){
                    List<Point> entries = ((PointLeafNode) curNode).getEntries();
                    for (Point entry: entries) {
                        if(entry.calcDistanceSquare(queryPoint) <= radius * radius){
                            result.add(entry);
                        }
                    }
                }
                else{
                    List<RTreeNode> childNodes = curNode.getChildNodes();
                    nodes.addAll(childNodes);
                }
            }
        }
        return result;
    }


    public List<Point> boxRange(MBR box){
        List<Point> result = new ArrayList<Point>();
        List<RTreeNode> nodes = new ArrayList<RTreeNode>();
        nodes.add(this.rootNode);
        while(!nodes.isEmpty()){
            RTreeNode curNode = nodes.remove(0);
            if(curNode.getMbr().intersects(box)){
                if(curNode.isLeaf()){
                    List<Point> entries = ((PointLeafNode) curNode).getEntries();
                    for (Point entry: entries) {
                        if(box.contains(entry)){
                            result.add(entry);
                        }
                    }
                }
                else{
                    List<RTreeNode> childNodes = curNode.getChildNodes();
                    nodes.addAll(childNodes);
                }
            }
        }
        return result;
    }

    public List<RTreeNode> getLeafNodes(){
        return searchLeafNodes(this.rootNode);
    }

    private List<RTreeNode> searchLeafNodes(RTreeNode node){
        List<RTreeNode> result = new ArrayList<RTreeNode>();
        List<RTreeNode> childNodes = node.getChildNodes();
        if(childNodes.get(0).isLeaf()){
            result.addAll(node.getChildNodes());
            return result;
        }
        else{
            for(int i =0; i< childNodes.size(); i++){
                result.addAll(searchLeafNodes(childNodes.get(i)));
            }
        }
        return result;
    }

    public List<Double> kNNDistance(final Point queryPoint, int k){
        List<Double> result = new ArrayList<Double>();
        int count = 0;
        PriorityQueue<RTreeNode> queue = new PriorityQueue<RTreeNode>(1, new Comparator<RTreeNode>() {
            @Override
            public int compare(RTreeNode o1, RTreeNode o2) {
                Double distance1 = o1.getMbr().calculateDistance(queryPoint);
                Double distance2 = o2.getMbr().calculateDistance(queryPoint);
                return distance1.compareTo(distance2);
            }
        });
        queue.add(this.rootNode);
        while(!queue.isEmpty()){
            RTreeNode curElement = queue.poll();
            if(curElement.isLeaf()){
                List<Point> leafPoints = ((PointLeafNode) curElement).getEntries();
                for(int i =0; i< leafPoints.size(); i++){
                    Point point = leafPoints.get(i);
                    result.add(point.calcDistance(queryPoint));
                }
                count += leafPoints.size();
            }
            else{
                queue.addAll(curElement.getChildNodes());
            }

            if(count >= k){
                Collections.sort(result);
                return result.subList(0, k);
            }
        }
        return result;
    }

    public List<Point> kNN(final Point queryPoint, int k){
        List<Point> result = new ArrayList<Point>();
        int count = 0;
        PriorityQueue<RTreeNode> queue = new PriorityQueue<RTreeNode>(1, new Comparator<RTreeNode>() {
            @Override
            public int compare(RTreeNode o1, RTreeNode o2) {
                Double distance1 = o1.getMbr().calculateDistance(queryPoint);
                Double distance2 = o2.getMbr().calculateDistance(queryPoint);
                return distance1.compareTo(distance2);
            }
        });
        PriorityQueue<Point> resultQueue = new PriorityQueue<Point>(1, new Comparator<Point>() {
            @Override
            public int compare(Point o1, Point o2) {
                Double distance1 = o1.calcDistance(queryPoint);
                Double distance2 = o2.calcDistance(queryPoint);
                return distance2.compareTo(distance1);
            }
        });
        queue.add(this.rootNode);
        while(!queue.isEmpty()){
            RTreeNode curElement = queue.poll();
            if(curElement.isLeaf()){
                    PointLeafNode leaf = (PointLeafNode) curElement;
                    Collections.sort(leaf.getEntries(), new Comparator<Point>() {
                        @Override
                        public int compare(Point o1, Point o2) {
                            Double distance1 = o1.calcDistance(queryPoint);
                            Double distance2 = o2.calcDistance(queryPoint);
                            return distance2.compareTo(distance1);
                        }
                    });
                    count += leaf.getEntries().size();
                    resultQueue.addAll(leaf.getEntries());
//                    for(int i =0; i< leaf.getEntries().size(); i++){
//                        System.out.println("Knn: " + leaf.getEntries().get(i));
//                    }
                }
                else{
                    queue.addAll(curElement.getChildNodes());
                }
        }

                while(count >k){
                    resultQueue.poll();
                    count--;
                }
                for(int i =0; i<count; i++){
                    result.add(resultQueue.poll());
                }

//        while(!queue.isEmpty()){
//            RTreeNode curElement = queue.poll();
//            double curDistance = curElement.getMbr().calculateDistance(queryPoint);
//            if(count >= k && (curDistance > kNNDistance)){
//                while(count >k){
//                    resultQueue.poll();
//                    count--;
//                }
//                for(int i =0; i<count; i++){
//                    result.add(resultQueue.poll());
//                }
//                return result;
//            }
//            else{
//                if(curElement.isLeaf()){
//                    PointLeafNode leaf = (PointLeafNode) curElement;
//                    Collections.sort(leaf.getEntries(), new Comparator<Point>() {
//                        @Override
//                        public int compare(Point o1, Point o2) {
//                            Double distance1 = o1.calcDistance(queryPoint);
//                            Double distance2 = o2.calcDistance(queryPoint);
//                            return distance2.compareTo(distance1);
//                        }
//                    });
//                    count += leaf.getEntries().size();
//                    resultQueue.addAll(leaf.getEntries());
////                    for(int i =0; i< leaf.getEntries().size(); i++){
////                        System.out.println("Knn: " + leaf.getEntries().get(i));
////                    }
//                    kNNDistance = leaf.getEntries().get(0).calcDistance(queryPoint);
//                }
//                else{
//                    queue.addAll(curElement.getChildNodes());
//                }
//            }
//        }

        return result;
    }

    public List<Point> kNN2(final Point queryPoint, int k){
        List<Double> distances = this.kNNDistance(queryPoint, k);
        double refined_bound = distances.get(distances.size()-1);
        List<Point> rangePoints = this.circleRange(queryPoint, refined_bound);
        Collections.sort(rangePoints, new Comparator<Point>() {
            @Override
            public int compare(Point o1, Point o2) {
                Double d1 = o1.calcDistance(queryPoint);
                Double d2 = o2.calcDistance(queryPoint);
                return d1.compareTo(d2);
            }
        });
        return rangePoints.subList(0, k);
    }

    public long getNumBytes() {
        return numBytes;
    }

    public void setNumBytes(long numBytes) {
        this.numBytes = numBytes;
    }
}
