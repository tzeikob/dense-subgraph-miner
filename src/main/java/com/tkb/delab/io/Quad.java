package com.tkb.delab.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * A writable comparable representing a quadruplet of integers.
 *
 * @author Akis Papadopoulos
 */
public class Quad implements WritableComparable<Quad> {
    
    public int v;
    
    public int u;
    
    public int w;
    
    public int z;

    /**
     * A constructor creating a quadruplet.
     */
    public Quad() {
    }

    /**
     * A constructor creating a quadruplet.
     *
     * @param v the first element value.
     * @param u the second element value.
     * @param w the third element value.
     * @param z the forth element value.
     */
    public Quad(int v, int u, int w, int z) {
        this.v = v;
        this.u = u;
        this.w = w;
        this.z = z;
    }

    /**
     * A method deserializing this quadruplet.
     *
     * @param in source for raw byte representation.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        v = in.readInt();
        u = in.readInt();
        w = in.readInt();
        z = in.readInt();
    }

    /**
     * A method serializing this quadruplet.
     *
     * @param out where to write the raw byte representation.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(v);
        out.writeInt(u);
        out.writeInt(w);
        out.writeInt(z);
    }

    /**
     * A method checking two quadruplet for equality.
     *
     * @param object the object comparing to.
     * @return true if object is equal to this object, false otherwise.
     */
    @Override
    public boolean equals(Object object) {
        Quad other = (Quad) object;

        return this.v == other.v && this.u == other.u && this.w == other.w && this.z == other.z;
    }

    /**
     * A method defining a natural sort order for quadruplets. Quadruplets are
     * sorted first by the first element, then by the second element, then by
     * the third and finally by the forth.
     *
     * @return a value less than zero, a value greater than zero, or zero if
     * this quadruplet should be sorted before, sorted after, or is equal to
     * object other.
     */
    @Override
    public int compareTo(Quad other) {
        if (this.v == other.v) {
            if (this.u == other.u) {
                if (this.w == other.w) {
                    if (this.z < other.z) {
                        return 1;
                    } else if (this.z > other.z) {
                        return -1;
                    } else {
                        return 0;
                    }
                } else if (this.w < other.w) {
                    return 1;
                } else {
                    return -1;
                }
            } else if (this.u < other.u) {
                return 1;
            } else {
                return -1;
            }
        } else if (this.v < other.v) {
            return 1;
        } else {
            return -1;
        }
    }

    /**
     * A method returning a hash code value for the quadruplet.
     *
     * @return hash code for the quadruplet.
     */
    @Override
    public int hashCode() {
        return v + u + w + z;
    }

    /**
     * A method generating a human-readable textual representation of this
     * quadruplet.
     *
     * @return human-readable textual representation of this quadruplet.
     */
    @Override
    public String toString() {
        return v + "," + u + "," + w + "," + z;
    }
}