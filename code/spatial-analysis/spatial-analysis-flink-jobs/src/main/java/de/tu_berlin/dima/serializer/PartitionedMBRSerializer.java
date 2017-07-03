package de.tu_berlin.dima.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import de.tu_berlin.dima.datatype.MBR;
import de.tu_berlin.dima.datatype.PartitionedMBR;

/**
 * Created by JML on 6/28/17.
 */
public class PartitionedMBRSerializer extends Serializer<PartitionedMBR> {
    @Override
    public void write(Kryo kryo, Output output, PartitionedMBR object) {
        kryo.writeObject(output, object.getMbr());
        output.writeInt(object.getPartitionNumber());
    }

    @Override
    public PartitionedMBR read(Kryo kryo, Input input, Class<PartitionedMBR> type) {
        MBR mbr = kryo.readObject(input, MBR.class);
        int partitionNumber = input.readInt();
        PartitionedMBR partitionedMBR = new PartitionedMBR(mbr, partitionNumber);
        partitionedMBR.setNumBytes(4 + mbr.getNumBytes());
        return partitionedMBR;
    }
}
