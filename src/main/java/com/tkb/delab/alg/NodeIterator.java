package com.tkb.delab.alg;

import com.tkb.delab.model.Edge;
import com.tkb.delab.model.Triangle;
import gnu.trove.iterator.hash.TObjectHashIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.THashSet;
import gnu.trove.set.hash.TIntHashSet;

/**
 * A triangulator implementing the node iteration plus algorithm listing all
 * triangles within a given graph, assuming there is no duplicate edges and no
 * loops. Be aware each edge must be ordered by vertices, the left vertex should
 * have id less than the id of the right vertex.
 *
 * @author Akis Papadopoulos
 */
public class NodeIterator implements Triangulator {

    /**
     * A method listing all triangles within a graph represented by a given edge
     * set using the node iteration plus method.
     *
     * @param edges edge set graph induced by.
     * @return a set of all triangles within the graph.
     */
    @Override
    public THashSet<Triangle> list(THashSet<Edge> edges) {
        // Creating an empty set of triangles
        THashSet<Triangle> triangles = new THashSet<Triangle>();

        // Creating an empty vertex neighborhood map
        TIntObjectHashMap<TIntHashSet> n = new TIntObjectHashMap<TIntHashSet>();

        // Getting the edge set iterator
        TObjectHashIterator eit = edges.iterator();

        // Iterating through the edge set
        while (eit.hasNext()) {
            // Getting the next edge
            Edge e = (Edge) eit.next();

            // Getting the first vertex
            int v = e.v;

            // Getting the second vertex
            int u = e.u;

            // Checking if the first vertex mapped already
            if (n.contains(v)) {
                // Adding it's next neighbor
                n.get(v).add(u);
            } else {
                // Creating an empty set of neighbors
                TIntHashSet set = new TIntHashSet();

                // Adding it's next neighbor
                set.add(u);

                // Mapping the vertex into the neighborhood map
                n.put(v, set);
            }

            // Checking if the second vertex mapped already
            if (n.contains(u)) {
                // Adding it's next neighbor
                n.get(u).add(v);
            } else {
                // Creating an empty set of neighbors
                TIntHashSet set = new TIntHashSet();

                // Adding it's next neighbor
                set.add(v);

                // Mapping the vertex into the neighborhood map
                n.put(u, set);
            }
        }

        // Getting the vertex set
        int[] vertices = n.keys();

        // Iterating through the vertex set
        for (int i = 0; i < vertices.length; i++) {
            // Getting the next vertex
            int v = vertices[i];

            // Getting the neighborhood of the vertex
            int[] nv = n.get(v).toArray();

            // Getting the degree of the vertex
            int dv = nv.length;

            // Iterating through all possible vertex neighbor pairs
            for (int j = 0; j < nv.length; j++) {
                // Getting the next neighbor
                int u = nv[j];

                // Getting the degree of the vertex
                int du = n.get(u).size();

                // Checking vertices by degree
                if (du > dv || (du == dv && v < u)) {
                    for (int k = 0; k < nv.length; k++) {
                        // Getting the next neighbor
                        int w = nv[k];

                        // Getting the degree of the vertex
                        int dw = n.get(w).size();

                        // Checking vertices by degree
                        if (dw > du || (dw == du && u < w)) {
                            // Creating the edge iinduced by the found vertex pair
                            Edge edge = new Edge(u, w);

                            // Sorting edge vertices
                            edge.sort();

                            // Checking if the edge exist
                            if (edges.contains(edge)) {
                                // Creating the found triangle
                                Triangle triangle = new Triangle(v, u, w);

                                // Sorting the triangle
                                triangle.sort();

                                // Adding the triangle into the set
                                triangles.add(triangle);
                            }
                        }
                    }
                }
            }
        }

        return triangles;
    }
}
