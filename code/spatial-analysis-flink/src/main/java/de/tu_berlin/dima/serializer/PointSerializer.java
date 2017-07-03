package de.tu_berlin.dima.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.tu_berlin.dima.datatype.Point;

/**
 * Created by JML on 6/21/17.
 */
public class PointSerializer extends Serializer<Point>{

    @Override
    public void write(Kryo kryo, Output output, Point object) {
        output.writeInt(object.getNbDimension());
        for(int i =0; i<object.getNbDimension(); i++){
            kryo.writeObject(output, object.getDimension(i));
        }
    }

    @Override
    public Point read(Kryo kryo, Input input, Class<Point> type) {
        int numDimension = input.readInt();
        float[] vals = new float[numDimension];
        for(int i =0; i< numDimension; i++){
            vals[i] = input.readFloat();
        }
        Point p = new Point(vals);
//        p.setNumbBytes( 4 * (numDimension +1));
        return p;
    }
}
