package com.tkb.delab.util;

/**
 * A math library of general use.
 *
 * @author Akis Papadopoulos
 */
public final class BasicMath {

    /**
     * A basic method to calculate the factorial of a number.
     *
     * @param x an integer number.
     * @return the factorial of the number.
     */
    public static long factorial(int x) {
        long fact = 1L;

        for (int i = 2; i <= x; i++) {
            fact *= i;
        }

        return fact;
    }
}
