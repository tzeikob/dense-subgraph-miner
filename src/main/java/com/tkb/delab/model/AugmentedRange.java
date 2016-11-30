package com.tkb.delab.model;

/**
 * A pair of lower and upper values representing an interval range of bounds
 * augmented by the abstract support value.
 *
 * @author Akis Papadopoulos
 */
public class AugmentedRange extends Range {

    //Support value
    public int support;

    /**
     * A constructor setting up an interval.
     *
     * @param lower the lower value.
     * @param upper the upper value.
     * @param support the support value.
     */
    public AugmentedRange(int lower, int upper, int support) {
        super(lower, upper);
        this.support = support;
    }

    /**
     * A method returning an alphanumeric representation of the interval.
     *
     * @return the alphanumeric representation of the interval.
     */
    @Override
    public String toString() {
        return lower + "," + upper + "," + support;
    }
}
