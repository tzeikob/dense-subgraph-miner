package com.tkb.delab.model;

/**
 * A pair of numerical vertices representing an edge.
 *
 * @author Akis Papadopoulos
 */
public class Edge {
    
    public int v;
    
    public int u;

    /**
     * A constructor setting up an edge.
     *
     * @param v the first vertex.
     * @param u the second vertex.
     */
    public Edge(int v, int u) {
        this.v = v;
        this.u = u;
    }

    /**
     * A method ordering the vertices by id within the edge in ascending order.
     */
    public void sort() {
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
        Edge other = (Edge) object;
        
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
