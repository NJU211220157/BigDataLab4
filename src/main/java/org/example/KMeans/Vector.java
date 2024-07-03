package org.example.KMeans;

public class Vector {

    private final double[] d = new double[16];

    public void set(int i, double v) {
        d[i] = v;
    }

    public double get(int i) {
        return d[i];
    }

    double computeDist(Vector t) {
        double res = 0;
        for (int i = 0; i < 16; i++) {
            res += (d[i] - t.get(i)) * (d[i] - t.get(i));
        }
        return res;
    }

    double mul(int v) {
        double res = 0;
        for (int i = 0; i < 16; i++) {
            res += d[i] * v;
        }
        return res;
    }

    void add(Vector t) {
        for (int i = 0; i < 16; i++) {
            d[i] += t.get(i);
        }
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 16; i++) {
            stringBuilder.append(d[i]);
            if (i != 15) {
                stringBuilder.append(", ");
            }
        }
        return stringBuilder.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof Vector)){
            return false;
        }
        for(int i = 0; i < 16; i++){
            if(this.get(i) != ((Vector) obj).get(i)){
                return false;
            }
        }
        return true;
    }
}
