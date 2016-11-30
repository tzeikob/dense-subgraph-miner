package com.tkb.delab.alg;

import com.tkb.delab.model.Edge;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.set.hash.THashSet;

/**
 * An interface for algorithms enumerating subgraphs induced by an edge set.
 *
 * @author Akis Papadopoulos
 */
public interface Enumerator {

    /**
     * An abstract method enumerating subgraphs induced by an edge set,
     * subgraphs mapped with an identifier number.
     *
     * @param edges a set of edges.
     * @return the list of subgraphs induced by edges within edge set.
     */
    public TIntObjectHashMap<THashSet<Edge>> enumerate(THashSet<Edge> edges);
}
