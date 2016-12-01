package com.tkb.delab.unit;

import com.tkb.delab.alg.Forward;
import com.tkb.delab.alg.Triangulator;
import com.tkb.delab.model.Edge;
import gnu.trove.set.hash.THashSet;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * A forward triangulation test case on graphs of various types.
 *
 * @author Akis Papadopoulos
 */
public class ForwardTestCase extends TriangulationTestCase {

    private final Triangulator triangulator = new Forward();

    @Test
    public void testTrivialGraph() {
        THashSet<Edge> edges = trivial.getEdges();
        int size = trivial.getNumberOfTriangles();

        THashSet triangles = triangulator.list(edges);

        assertTrue("Graph " + trivial + " should have " + size + " triangles.", triangles.size() == size);
    }

    @Test
    public void testDisconnectedGraph() {
        THashSet<Edge> edges = disconnected.getEdges();
        int size = disconnected.getNumberOfTriangles();

        THashSet triangles = triangulator.list(edges);

        assertTrue("Graph " + disconnected + " should have " + size + " triangles.", triangles.size() == size);
    }

    @Test
    public void testDirectedGraph() {
        THashSet<Edge> edges = directed.getEdges();
        int size = directed.getNumberOfTriangles();

        THashSet triangles = triangulator.list(edges);

        assertTrue("Graph " + directed + " should have " + size + " triangles.", triangles.size() == size);
    }

    @Test
    public void testUndirectedGraph() {
        THashSet<Edge> edges = undirected.getEdges();
        int size = undirected.getNumberOfTriangles();

        THashSet triangles = triangulator.list(edges);

        assertTrue("Graph " + undirected + " should have " + size + " triangles.", triangles.size() == size);
    }

    @Test
    public void testCycleGraph() {
        THashSet<Edge> edges = cycle.getEdges();
        int size = cycle.getNumberOfTriangles();

        THashSet triangles = triangulator.list(edges);

        assertTrue("Graph " + cycle + " should have " + size + " triangles.", triangles.size() == size);
    }

    @Test
    public void testWheelGraph() {
        THashSet<Edge> edges = wheel.getEdges();
        int size = wheel.getNumberOfTriangles();

        THashSet triangles = triangulator.list(edges);

        assertTrue("Graph " + wheel + " should have " + size + " triangles.", triangles.size() == size);
    }

    @Test
    public void testStarGraph() {
        THashSet<Edge> edges = star.getEdges();
        int size = star.getNumberOfTriangles();

        THashSet triangles = triangulator.list(edges);

        assertTrue("Graph " + star + " should have " + size + " triangles.", triangles.size() == size);
    }
}
