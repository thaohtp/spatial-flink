package str_test;

import de.tu_berlin.dima.datatype.Point;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by JML on 4/24/17.
 */
public class TestUtil {
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
}
