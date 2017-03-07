package flink;

import flink.datatype.Point;
import flink.datatype.PointComparator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by JML on 3/7/17.
 */
public class STRPacking {
    List<Point> pointList;
    int pointPerNode;
    int nbDimension;

    public STRPacking(int nbPointPerNode, int nbDimension, List<Point> pointList){
        this.pointList = pointList;
        this.pointPerNode = nbPointPerNode;
        this.nbDimension = nbDimension;
    }

    public void sort(){
        // sort by first dimension

        for(int i =0; i< nbDimension; i++){
            // calculate dimension: dimension is varies from 1 to n
            int k = i + 1;

            // calculate P = r/n with r = size of dataset, n = number of points per node
            int P =0;
            if(this.pointList.size() % this.pointPerNode == 0 ){
                P = this.pointList.size() /this.pointPerNode;
            }
            else{
                P = this.pointList.size() /this.pointPerNode + 1;
            }

            int S = 1;
            if(k != 1){
                float temp = 1L/(k-1);
                S = (int) Math.pow(P, temp);
            }

            // sort start index, sort end index
            int pointPerSlab = this.pointList.size();
            if(S != 1){
                pointPerSlab = (int) Math.ceil(Math.sqrt(this.pointList.size() / (S-1)));
            }
            for(int j =0; j < S; j++){
                int startIndex = pointPerSlab * j;
                int endIndex = pointPerSlab * (j+1) -1;
                List<Point> subList = new ArrayList<Point>();
                for(int m = startIndex; m <= endIndex; m++){
                    subList.add(this.pointList.remove(startIndex));
                }
                Collections.sort(subList, new PointComparator(k-1));
                for(int m = startIndex; m <= endIndex; m++) {
                    pointList.add(m, subList.remove(0));
                }
            }

            // sort those point
        }
    }

    public Point getPoint(int index){
        return this.pointList.get(index);
    }

    public int getSize(){
        return this.pointList.size();
    }

}
