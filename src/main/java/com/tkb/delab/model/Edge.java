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
package com.tkb.delab.model;

/**
 * A pair of numerical vertices representing an edge.
 *
 * @author Akis Papadopoulos
 */
public class Edge {

    //First vertex
    public int v;
    //Second vertex
    public int u;

    /**
     * A constructor setting up an edge.
     *
     * @param v the first vertex.
     * @param u the second vertex.
     */
    public Edge(int v, int u) {
        //Setting the first vertex
        this.v = v;

        //Setting the second vertex
        this.u = u;
    }

    /**
     * A method ordering the vertices by id within the edge in ascending order.
     */
    public void sort() {
        //Ordering vertices by id in ascending order
        if (u < v) {
            int t = v;
            v = u;
            u = t;
        }
    }

    /**
     * A method checking if the edge is equal to the given object.
     *
     * @param object the given edge as generic object.
     * @return if the edge is equal to the given edge.
     */
    @Override
    public boolean equals(Object object) {
        //Casting the object to an edge
        Edge other = (Edge) object;

        //Checking if the end vertices are equal
        if (this.v == other.v && this.u == other.u) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * A basic method calculating the hash code of the edge.
     *
     * @return the hash code of the edge.
     */
    @Override
    public int hashCode() {
        int hash = 3;
        hash = 83 * hash + this.v;
        hash = 83 * hash + this.u;

        return hash;
    }

    /**
     * A method returning an alphanumeric representation of the edge.
     *
     * @return the alphanumeric representation of the edge.
     */
    @Override
    public String toString() {
        return v + "," + u;
    }
}
