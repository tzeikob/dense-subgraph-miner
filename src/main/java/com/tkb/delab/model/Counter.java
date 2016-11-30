package com.tkb.delab.model;

/**
 * A set of counters.
 *
 * @author Akis Papadopoulos
 */
public enum Counter {

    //Number of edges not converged to an optimal valid lambda bound
    UNCONVERGED_EDGES,
    //Number of edges converged to an optimal valid lambda bound
    CONVERGED_EDGES,
    //Total sum of the lambda each edge has
    SUM_OF_LAMBDA
}
