/*
 * Miner: Dense Subgraph Enumeration MapReduce Tool
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.tkb.delab.alg;

import com.tkb.delab.model.AugmentedRange;
import com.tkb.delab.model.Edge;
import com.tkb.delab.model.Triangle;
import gnu.trove.iterator.hash.TObjectHashIterator;
import gnu.trove.map.hash.THashMap;
import gnu.trove.set.hash.THashSet;
import java.util.Map.Entry;

/**
 * An edge neighborhood density estimator based on triangulation using a binary
 * search mode.
 *
 * @author Akis Papadopoulos
 */
public class BinaryEstimator implements EdgeDensityEstimator {

    //Maximum number of iterations
    private int iterations;

    /**
     * A default constructor creating a binary edge density estimator.
     */
    public BinaryEstimator() {
        this.iterations = 10;
    }

    /**
     * A constructor creating a binary edge density estimator.
     *
     * @param iterations the maximum numbers of iterations to converge.
     */
    public BinaryEstimator(int iterations) {
        this.iterations = iterations;
    }

    /**
     * A method scoring for each edge the neighborhood density, given the edge
     * set and the triangles within, using a binary search mode.
     *
     * @param triangles a set of the triangles within the graph.
     * @return a hash map between edge and its lambda density score.
     */
    @Override
    public THashMap<Edge, AugmentedRange> estimate(THashSet<Triangle> triangles) {
        //Creating an empty map between edges and lambda range
        THashMap<Edge, AugmentedRange> edges = new THashMap<Edge, AugmentedRange>();

        //Iterating through the triangles initializing each edge lambda bounds
        for (Triangle t : triangles) {
            //Creating the first sorted edge
            Edge e1 = new Edge(t.v, t.u);

            //Checking if the edge mapped already
            if (edges.contains(e1)) {
                //Incrementing the lambda upper value
                edges.get(e1).upper += 1;
            } else {
                //Mapping the edge into the hash map
                edges.put(e1, new AugmentedRange(1, 1, 0));
            }

            //Creating the second sorted edge
            Edge e2 = new Edge(t.u, t.w);

            //Checking if the edge mapped already
            if (edges.contains(e2)) {
                //Incrementing the lambda upper value
                edges.get(e2).upper += 1;
            } else {
                //Mapping the edge into the hash map
                edges.put(e2, new AugmentedRange(1, 1, 0));
            }

            //Creating the third sorted edge
            Edge e3 = new Edge(t.v, t.w);

            //Checking if the edge mapped already
            if (edges.contains(e3)) {
                //Incrementing the lambda upper value
                edges.get(e3).upper += 1;
            } else {
                //Mapping the edge into the hash map
                edges.put(e3, new AugmentedRange(1, 1, 0));
            }
        }

        //Defining a converged indicator
        boolean converged;

        //Defining an index of the current iteration
        int it = 0;

        //Iterating till converged or reached the maximum number of iterations
        do {
            //Setting to converged at the beginning of the iteration
            converged = true;

            //Gettign the triangles iterator
            TObjectHashIterator<Triangle> tit = triangles.iterator();

            //Iterating throught the triangles claculating the support values
            while (tit.hasNext()) {
                //Getting the next triangle
                Triangle triangle = tit.next();

                //Creating an empty list of edges
                Edge[] e = new Edge[3];

                //Creating the first edge
                e[0] = new Edge(triangle.v, triangle.u);

                //Creating the second edge
                e[1] = new Edge(triangle.u, triangle.w);

                //Creating the third edge
                e[2] = new Edge(triangle.v, triangle.w);

                //Iterating through the edges of the triangle
                for (int i = 0; i < e.length; i++) {
                    //Getting the next edge lambda bound range
                    AugmentedRange range = edges.get(e[i]);

                    //Getting the pivot edge lambda bound medium
                    int mi = range.medium();

                    //Getting the first other edge lambda bound medium
                    int m1 = edges.get(e[(i + 1) % 3]).medium();

                    //Getting the second other edge lambda bound medium
                    int m2 = edges.get(e[(i + 2) % 3]).medium();

                    //checking if the medium is less than the minimum of the others
                    if (mi <= Math.min(m1, m2)) {
                        //Incrementig the support value of the edge
                        range.support += 1;
                    }
                }
            }

            //Iterating through the edges
            for (Entry<Edge, AugmentedRange> entry : edges.entrySet()) {
                //Getting the lambda bound range of the next edge
                AugmentedRange range = entry.getValue();

                //Checking if the edge converged to a valid lambda bound
                if (range.lower < range.upper) {
                    //Getting the medium of the lambda bound range
                    int m = range.medium();

                    //Updating the bounds of the lambda range
                    if (range.support < m) {
                        range.upper = m - 1;
                    } else {
                        range.lower = m;
                    }

                    //Marking the process as not converged
                    converged = false;
                }

                //Resetting the support of the edge
                range.support = 0;
            }

            //Incrementing to the next iteration
            it++;
        } while (!converged && it <= iterations - 1);

        return edges;
    }
}
