package flink.datagen.util;


public interface SymmetricPRNG {

    void seed(long seed);

    void skipTo(long pos);

    double next();

    int nextInt(int k);
}
