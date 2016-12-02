package com.tkb.delab.model;

/**
 * A pair of numerical vertices followed by its lambda bounds representing an
 * augmented edge.
 *
 * @author Akis Papadopoulos
 */
public class LambdaEdge extends Edge {

    public int kappa;
    
    public int lambda;

    /**
     * A constructor setting up an edge.
     *
     * @param v the first vertex.
     * @param u the second vertex.
     * @param kappa the lower lambda bound.
     * @param lambda the upper lambda bound.
     */
    public LambdaEdge(int v, int u, int kappa, int lambda) {
        super(v, u);
        
        this.kappa = kappa;
        this.lambda = lambda;
    }

    /**
     * A method returning an alphanumeric representation of the edge.
     *
     * @return the alphanumeric representation of the edge.
     */
    @Override
    public String toString() {
        return v + "," + u + "," + kappa + "," + lambda;
    }
}
