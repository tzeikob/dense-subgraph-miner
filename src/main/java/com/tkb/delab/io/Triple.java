/*
 * Miner: Dense Subgraph Enumeration MapReduce Tool
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.tkb.delab.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 * A writable comparable representing a triple of integers.
 *
 * @author Akis Papadopoulos
 */
public class Triple implements WritableComparable<Triple> {

    //First element value
    public int v;
    //Second element value
    public int u;
    //Third element value
    public int w;

    /**
     * A constructor creating a triple.
     */
    public Triple() {
    }

    /**
     * A constructor creating a triple.
     *
     * @param v the first element value.
     * @param u the second element value.
     * @param w the third element value.
     */
    public Triple(int v, int u, int w) {
        this.v = v;
        this.u = u;
        this.w = w;
    }

    /**
     * A method deserializing this triple.
     *
     * @param in source for raw byte representation.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        v = in.readInt();
        u = in.readInt();
        w = in.readInt();
    }

    /**
     * A method serializing this triple.
     *
     * @param out where to write the raw byte representation.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(v);
        out.writeInt(u);
        out.writeInt(w);
    }

    /**
     * A method checking two triples for equality.
     *
     * @param object the object comparing to.
     * @return true if object is equal to this object, false otherwise.
     */
    @Override
    public boolean equals(Object object) {
        //Casting the object to triple
        Triple other = (Triple) object;

        return this.v == other.v && this.u == other.u && this.w == other.w;
    }

    /**
     * A method defining a natural sort order for triples. Triples are sorted
     * first by the first element, then by the second element and finally by the
     * third.
     *
     * @return a value less than zero, a value greater than zero, or zero if
     * this triple should be sorted before, sorted after, or is equal to object
     * other.
     */
    @Override
    public int compareTo(Triple other) {
        //Checking the first element
        if (this.v == other.v) {
            //Checking the second elements
            if (this.u == other.u) {
                //Checking the thrid elements
                if (this.w < other.w) {
                    return 1;
                } else if (this.w > other.w) {
                    return -1;
                } else {
                    return 0;
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
     * A method returning a hash code value for the triple.
     *
     * @return hash code for the triple.
     */
    @Override
    public int hashCode() {
        return v + u + w;
    }

    /**
     * A method generating a human-readable textual representation of this
     * triple.
     *
     * @return human-readable textual representation of this triple.
     */
    @Override
    public String toString() {
        return v + "," + u + "," + w;
    }
}