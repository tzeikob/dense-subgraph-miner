package com.tkb.delab.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * A writable comparable representing a pair of integers.
 *
 * @author Akis Papadopoulos
 */
public class Pair implements WritableComparable<Pair> {

    //First element value
    public int v;
    //Second element value
    public int u;

    /**
     * A constructor creating a pair.
     */
    public Pair() {
    }

    /**
     * A constructor creating a pair.
     *
     * @param v the first element value.
     * @param u the second element value.
     */
    public Pair(int v, int u) {
        this.v = v;
        this.u = u;
    }

    /**
     * A method deserializing this pair.
     *
     * @param in source for raw byte representation.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        v = in.readInt();
        u = in.readInt();
    }

    /**
     * A method serializing this pair.
     *
     * @param out where to write the raw byte representation.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(v);
        out.writeInt(u);
    }

    /**
     * A method checking two pairs for equality.
     *
     * @param object the object comparing to.
     * @return true if object is equal to this object, false otherwise.
     */
    @Override
    public boolean equals(Object object) {
        //Casting the object to pair
        Pair other = (Pair) object;

        return this.v == other.v && this.u == other.u;
    }

    /**
     * A method defining a natural sort order for pairs. Pairs are sorted first
     * by the first element, and then by the second element.
     *
     * @return a value less than zero, a value greater than zero, or zero if
     * this pair should be sorted before, sorted after, or is equal to object
     * other.
     */
    @Override
    public int compareTo(Pair other) {
        //Checking the first element
        if (this.v == other.v) {
            //Checking the second elements
            if (this.u < other.u) {
                return 1;
            } else if (this.u > other.u) {
                return -1;
            } else {
                return 0;
            }
        } else if (this.v < other.v) {
            return 1;
        } else {
            return -1;
        }
    }

    /**
     * A method returning a hash code value for the pair.
     *
     * @return hash code for the pair.
     */
    @Override
    public int hashCode() {
        return v + u;
    }

    /**
     * A method generating a human-readable textual representation of this pair.
     *
     * @return human-readable textual representation of this pair.
     */
    @Override
    public String toString() {
        return v + "," + u;
    }
}