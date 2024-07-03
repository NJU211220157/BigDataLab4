package org.example.KMeans;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VectorPair implements Writable {

    private Vector vector;

    private int count;

    public VectorPair(){
        this.vector = new Vector();
    }

    public VectorPair(Vector vector, int count) {
        this.vector = vector;
        this.count = count;
    }

    public Vector getVector() {
        return vector;
    }

    public int getCount() {
        return count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        for(int i = 0; i < 16; i++){
            dataOutput.writeDouble(vector.get(i));
        }
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        for (int i = 0; i < 16; i++){
            this.vector.set(i, dataInput.readDouble());
        }
        this.count = dataInput.readInt();
    }
}
