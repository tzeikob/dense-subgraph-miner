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
        //Initializing the factorial
        long fact = 1L;

        //Iterating through all the multiples
        for (int i = 2; i <= x; i++) {
            //Updating the factorial value
            fact *= i;
        }

        return fact;
    }
}
