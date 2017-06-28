package de.tu_berlin.dima.util;

import de.tu_berlin.dima.RTree;
import de.tu_berlin.dima.datatype.*;
import de.tu_berlin.dima.serializer.*;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 3/7/17.
 */
public class Utils {
    public static Point create2DPoint(float x, float y){
        List<Float> floatList = new ArrayList<Float>(2);
        floatList.add(0, x);
        floatList.add(1, y);
        return new Point(floatList);
    }

    public static Point create3DPoint(float x, float y, float z){
        List<Float> floatList = new ArrayList<Float>(3);
        floatList.add(0, x);
        floatList.add(1, y);
        floatList.add(2, z);
        return new Point(floatList);
    }

    public static ExecutionEnvironment registerTypeWithKryoSerializer(ExecutionEnvironment env){
        Class[] kryoTypeClasses = new Class[]{Point.class,
                MBR.class,
                MBRLeafNode.class,
                NonLeafNode.class,
                PartitionedMBR.class,
                Point.class,
                PointLeafNode.class,
                RTreeNode.class};
        for(int i =0; i< kryoTypeClasses.length; i++){
            env.getConfig().registerKryoType(kryoTypeClasses[i]);
        }
        return env;
    }

    public static ExecutionEnvironment registerCustomSerializer(ExecutionEnvironment env){
        env.registerTypeWithKryoSerializer(Point.class, PointSerializer.class);
        env.registerTypeWithKryoSerializer(MBR.class, MBRSerializer.class);
        env.registerTypeWithKryoSerializer(PartitionedMBR.class, PartitionedMBRSerializer.class);
        env.registerTypeWithKryoSerializer(LeafNode.class, RTreeNodeSerializer.class);
        env.registerTypeWithKryoSerializer(PointLeafNode.class, RTreeNodeSerializer.class);
        env.registerTypeWithKryoSerializer(MBRLeafNode.class, RTreeNodeSerializer.class);
        env.registerTypeWithKryoSerializer(NonLeafNode.class, RTreeNodeSerializer.class);
        env.registerTypeWithKryoSerializer(RTreeNode.class, RTreeNodeSerializer.class);
        env.registerTypeWithKryoSerializer(RTree.class, RTreeSerializer.class);
        return env;
    }
}
