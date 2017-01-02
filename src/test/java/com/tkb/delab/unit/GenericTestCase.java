package com.tkb.delab.unit;

import com.tkb.delab.model.Triangle;
import gnu.trove.set.hash.THashSet;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * A test case for generic purposes.
 *
 * @author Akis Papadopoulos
 */
public class GenericTestCase extends TriangulationTestCase {

    @Test
    public void testTriangleHashSet() {
        THashSet<Triangle> triangles = new THashSet<Triangle>();
        triangles.add(new Triangle(2, 5, 6));
        triangles.add(new Triangle(2, 5, 6));

        assertTrue("Triangle set should have 1 triangle.", triangles.size() == 1);
    }
}
