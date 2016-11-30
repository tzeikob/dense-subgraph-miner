package com.tkb.delab.model;

/**
 * A pair of lower and upper values representing an interval range of bounds.
 * 
 * @author Akis Papadopoulos
 */
public class Range {
    
    //Lower bound value
    public int lower;
    //Upper bound value
    public int upper;

    /**
     * A constructor setting up the interval.
     * 
     * @param lower the lower bound.
     * @param upper the upper bound.
     */
    public Range(int lower, int upper) {
        this.lower = lower;
        this.upper = upper;
    }

    /**
     * A method returning the medium of the interval.
     *
     * @return the medium of the lower and upper bounds.
     */
    public int medium() {
        return (lower + upper + 1) / 2;
    }

    /**
     * A method returning an alphanumeric representation of the interval.
     *
     * @return the alphanumeric representation of the interval.
     */
    @Override
    public String toString() {
        return lower + "," + upper;
    }
}
