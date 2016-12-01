package com.tkb.delab.unit;

import com.tkb.delab.model.Edge;
import gnu.trove.set.hash.THashSet;

/**
 * A ground truth graph definition given the list of the edges and the number of
 * triangles should contain.
 *
 * @author Akis Papadopoulos
 */
public class GraphDefinition {

    private final THashSet<Edge> edges;

    private final int numberOfTriangles;

    public GraphDefinition(final int numberOfTriangles) {
        edges = new THashSet<Edge>();
        this.numberOfTriangles = numberOfTriangles;
    }

    public void addEdge(Edge edge) {
        edges.add(edge);
    }

    public THashSet<Edge> getEdges() {
        return edges;
    }

    public int getNumberOfTriangles() {
        return numberOfTriangles;
    }

    @Override
    public String toString() {
        return edges.toString();
    }
}
