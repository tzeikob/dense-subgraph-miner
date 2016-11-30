package com.tkb.delab.model;

/**
 * A triplet of numerical vertices representing a triangle.
 *
 * @author Akis Papadopoulos
 */
public class Triangle {

    //First vertex
    public int v;
    //Second vertex
    public int u;
    //Third vertex
    public int w;

    /**
     * A constructor setting up a triangle.
     *
     * @param v the first vertex.
     * @param u the second vertex.
     * @param w the third vertex.
     */
    public Triangle(int v, int u, int w) {
        //Setting the first vertex
        this.v = v;

        //Setting the second vertex
        this.u = u;

        //Setting the third vertex
        this.w = w;
    }

    /**
     * A method sorting the vertices by id within the triangle in ascending
     * order.
     */
    public void sort() {
        //Sorting vertices by id in ascending order
        if (u < v) {int t = u; u = v; v = t;}
        if (w < u) {int t = w; w = u; u = t;}        
        if (u < v) {int t = u; u = v; v = t;}
    }

    /**
     * A method checking if the triangle is equal to the given object.
     *
     * @param object the given triangle as generic object.
     * @return if the triangle is equal to the given triangle.
     */
    @Override
    public boolean equals(Object object) {
        //Casting the object to a triangle
        Triangle other = (Triangle) object;

        //Checking if the end vertices are equal
        if (this.v == other.v && this.u == other.u && this.w == other.w) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * A basic method calculating the hash code of the triangle.
     *
     * @return the hash code of the triangle.
     */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 67 * hash + this.v;
        hash = 67 * hash + this.u;
        hash = 67 * hash + this.w;

        return hash;
    }

    /**
     * A method returning an alphanumeric representation of the triangle.
     *
     * @return the alphanumeric representation of the triangle.
     */
    @Override
    public String toString() {
        return v + "," + u + "," + w;
    }
}
