package de.tu_berlin.dima.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.tu_berlin.dima.datatype.MBR;
import de.tu_berlin.dima.datatype.Point;

/**
 * Created by JML on 6/28/17.
 */
public class MBRSerializer extends Serializer<MBR> {
    @Override
    public void write(Kryo kryo, Output output, MBR object) {
        kryo.writeObject(output, object.getMaxPoint());
        kryo.writeObject(output, object.getMinPoint());
        output.writeBoolean(object.isInitialized());
        output.writeInt(object.getNbDimension());
    }

    @Override
    public MBR read(Kryo kryo, Input input, Class<MBR> type) {
        Point maxPoint = kryo.readObject(input, Point.class);
        Point minPoint = kryo.readObject(input, Point.class);
        MBR mbr = new MBR(maxPoint, minPoint);
        mbr.setInitialized(input.readBoolean());
        mbr.setNbDimension(input.readInt());
//        mbr.setNumBytes(maxPoint.getNumbBytes() + minPoint.getNumbBytes() + 1 + 4);
        return mbr;
    }
}
