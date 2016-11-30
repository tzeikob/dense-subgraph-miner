package com.tkb.delab.unit;

import com.tkb.delab.alg.BinaryEstimator;
import com.tkb.delab.alg.EdgeDensityEstimator;
import com.tkb.delab.alg.Forward;
import com.tkb.delab.alg.Triangulator;
import com.tkb.delab.model.Edge;
import gnu.trove.set.hash.THashSet;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Triangulation test drives.
 *
 * @author Akis Papadopoulos
 */
public class Tester {

    public static void main(String[] args) throws Exception {
        THashSet<Edge> e = new THashSet<Edge>();
        BufferedReader input = new BufferedReader(new FileReader(new File("/home/akis/Downloads/ego-facebook")));
        String line = null;
        while ((line = input.readLine()) != null) {
            int v = Integer.parseInt(line.split(",")[0]);
            int u = Integer.parseInt(line.split(",")[1]);
            if (v != u) {
                Edge edge = new Edge(v, u);
                edge.sort();
                e.add(edge);
            }
        }
        System.out.println("Edgeset.size: " + e.size());

        //Triangulator bt = new NodeIterator();
        //long start = System.currentTimeMillis();
        //THashSet t = bt.list(e);
        //long end = System.currentTimeMillis();
        //System.out.println("Nit: " + t.size() + " in " + (end - start) + "ms");

        Triangulator frw = new Forward();
        long start = System.currentTimeMillis();
        THashSet t3 = frw.list(e);
        long end = System.currentTimeMillis();
        System.out.println("Frw: " + t3.size() + " in " + (end - start) + "ms");
        
        EdgeDensityEstimator dngraph = new BinaryEstimator(15);
        dngraph.estimate(t3);
    }
}
