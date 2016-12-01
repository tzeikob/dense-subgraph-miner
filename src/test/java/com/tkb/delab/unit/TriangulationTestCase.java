package com.tkb.delab.unit;

import com.tkb.delab.model.Edge;
import org.junit.Before;

/**
 * A triangulation test case on various type graphs of 4 vertices.
 *
 * @author Akis Papadopoulos
 */
public class TriangulationTestCase {

    protected GraphDefinition trivial;

    protected GraphDefinition disconnected;

    protected GraphDefinition directed;

    protected GraphDefinition undirected;

    protected GraphDefinition cycle;

    protected GraphDefinition wheel;

    protected GraphDefinition star;

    @Before
    public void init() {
        // Building a trivial graph contains no triangles
        trivial = new GraphDefinition(0);

        // Building a disconnected graph with cycles contains no triangles
        disconnected = new GraphDefinition(0);

        disconnected.addEdge(new Edge(1, 2));
        disconnected.addEdge(new Edge(1, 2));
        disconnected.addEdge(new Edge(3, 4));
        disconnected.addEdge(new Edge(3, 4));

        // Building a complete directed graph contains 4 triangles
        directed = new GraphDefinition(4);

        directed.addEdge(new Edge(1, 2));
        directed.addEdge(new Edge(1, 3));
        directed.addEdge(new Edge(1, 4));
        directed.addEdge(new Edge(2, 3));
        directed.addEdge(new Edge(2, 4));
        directed.addEdge(new Edge(3, 4));

        // Building a complete undirected graph contains 4 triangles
        undirected = new GraphDefinition(4);

        undirected.addEdge(new Edge(1, 2));
        undirected.addEdge(new Edge(1, 3));
        undirected.addEdge(new Edge(1, 4));
        undirected.addEdge(new Edge(1, 2));
        undirected.addEdge(new Edge(2, 3));
        undirected.addEdge(new Edge(2, 4));
        undirected.addEdge(new Edge(1, 3));
        undirected.addEdge(new Edge(2, 3));
        undirected.addEdge(new Edge(3, 4));
        undirected.addEdge(new Edge(1, 4));
        undirected.addEdge(new Edge(2, 4));
        undirected.addEdge(new Edge(3, 4));

        // Building a cycle graph contains no triangles
        cycle = new GraphDefinition(0);

        cycle.addEdge(new Edge(1, 2));
        cycle.addEdge(new Edge(2, 3));
        cycle.addEdge(new Edge(3, 4));
        cycle.addEdge(new Edge(1, 4));

        // Building a wheel graph contains 4 triangles
        wheel = new GraphDefinition(4);

        wheel.addEdge(new Edge(1, 4));
        wheel.addEdge(new Edge(2, 4));
        wheel.addEdge(new Edge(3, 4));
        wheel.addEdge(new Edge(1, 2));
        wheel.addEdge(new Edge(1, 3));
        wheel.addEdge(new Edge(2, 3));

        // Building a star graph contains no triangles
        star = new GraphDefinition(0);

        star.addEdge(new Edge(1, 4));
        star.addEdge(new Edge(2, 4));
        star.addEdge(new Edge(3, 4));
    }
}
