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

import com.tkb.delab.model.Edge;
import com.tkb.delab.model.Triangle;
import gnu.trove.iterator.hash.TObjectHashIterator;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.THashSet;
import gnu.trove.set.hash.TIntHashSet;
import java.util.Arrays;

/**
 * A triangulator implementing the forward algorithm listing all triangles
 * within a given graph, assuming there is no duplicate edges and no loops.
 *
 * @author Akis Papadopoulos
 */
public class Forward implements Triangulator {

    /**
     * A method listing all triangles within a graph represented by a given edge
     * set using the forward method.
     *
     * @param edges edge set graph induced by.
     * @return a set of all triangles within the graph.
     */
    @Override
    public THashSet<Triangle> list(THashSet<Edge> edges) {
        //Creating an empty set of triangles
        THashSet<Triangle> triangles = new THashSet<Triangle>();

        //Creating an empty vertex neighborhood map
        TIntObjectHashMap<TIntHashSet> n = new TIntObjectHashMap<TIntHashSet>();

        //Getting the edge set iterator
        TObjectHashIterator eit = edges.iterator();

        //Iterating through the edge set
        while (eit.hasNext()) {
            //Getting the next edge
            Edge e = (Edge) eit.next();

            //Getting the first vertex
            int v = e.v;

            //Getting the second vertex
            int u = e.u;

            //Checking if the first vertex mapped already
            if (n.contains(v)) {
                //Adding it's next neighbor
                n.get(v).add(u);
            } else {
                //Creating an empty set of neighbors
                TIntHashSet set = new TIntHashSet();

                //Adding it's next neighbor
                set.add(u);

                //Mapping the vertex into the neighborhood map
                n.put(v, set);
            }

            //Checking if the second vertex mapped already
            if (n.contains(u)) {
                //Adding it's next neighbor
                n.get(u).add(v);
            } else {
                //Creating an empty set of neighbors
                TIntHashSet set = new TIntHashSet();

                //Adding it's next neighbor
                set.add(v);

                //Mapping the vertex into the neighborhood map
                n.put(u, set);
            }
        }

        //Creating an empty vertex degree bucket map
        TIntObjectHashMap<TIntHashSet> d = new TIntObjectHashMap<TIntHashSet>();

        //Getting the vertex set
        int[] vertices = n.keys();

        //Iterating through the vertex set
        for (int i = 0; i < vertices.length; i++) {
            //Gettign the next vertex
            int v = vertices[i];

            //Getting it's degree
            int dv = n.get(v).size();

            //Checking if the vertex degree is already mapped
            if (d.contains(dv)) {
                //Adding the vertex into degree set
                d.get(dv).add(v);
            } else {
                //Creating an empty degree set
                TIntHashSet set = new TIntHashSet();

                //Adding the vertex into the degree set
                set.add(v);

                //Adding the degree into the map
                d.put(dv, set);
            }
        }

        //Creating an empty vertex rank map
        TIntIntHashMap r = new TIntIntHashMap();

        //Setting the first rank
        int rank = 1;

        //Iterating through the possible vertex degree set
        for (int di = vertices.length - 1; di > 0; di--) {
            //Checking if the next degree is mapped
            if (d.contains(di)) {
                //Getting the vertex set of the next degree
                int[] v = d.get(di).toArray();

                //Sorting vertices by id in ascending order
                Arrays.sort(v);

                //Iterating through the vertex set
                for (int i = 0; i < v.length; i++) {
                    //Mapping next vertex into the map with it's rank
                    r.put(rank, v[i]);

                    //Updating to the next rank
                    rank++;
                }
            }
        }

        //Creating an empty beta vertex neighborhood map
        TIntObjectHashMap<TIntHashSet> a = new TIntObjectHashMap<TIntHashSet>();

        //Iterating through the ranked vertex set
        for (int i = 1; i <= vertices.length; i++) {
            //Getting i-th ranked vertex
            int v = r.get(i);

            //Getting the vertex neighborhood
            int[] nv = n.get(v).toArray();

            //Storing the vertex's degree
            int dv = nv.length;

            //Iterating through it's neighbors
            for (int j = 0; j < nv.length; j++) {
                //Getting the nexet neighbor
                int u = nv[j];

                //Getting neighbor's degree
                int du = n.get(u).size();

                //Checking the degrees of the vertices
                if (du < dv || (du == dv && v < u)) {
                    //Getting the current beta neighborhood of u
                    TIntHashSet au = a.get(u);

                    //Getting the current beta neighborhood of v
                    TIntHashSet av = a.get(v);

                    //Checking if the u's beta neighborhood exists
                    if (au != null) {
                        //Checking if the v's beta neighborhood exists
                        if (av != null) {
                            //Getting the u's beta neighbors
                            int[] w = au.toArray();

                            //Iterating through the beta neighbors of u
                            for (int k = 0; k < w.length; k++) {
                                //Checking if next beta neighbor contained within v's beta hood
                                if (av.contains(w[k])) {
                                    //Creating a new triangle induced by the found vertices
                                    Triangle triangle = new Triangle(v, u, w[k]);

                                    //Ordering vertices within the triangle
                                    triangle.sort();

                                    //Adding found triangle into the set
                                    triangles.add(triangle);
                                }
                            }
                        }

                        //Adding vertex v into the u's beta neighborhood
                        au.add(v);
                    } else {
                        //Creating an empty set of neighbors
                        TIntHashSet set = new TIntHashSet();

                        //Adding the vertex v into the neighbors
                        set.add(v);

                        //Putting the u's neighborhood into the map
                        a.put(u, set);
                    }
                }
            }
        }

        return triangles;
    }
}
