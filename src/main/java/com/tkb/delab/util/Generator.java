package com.tkb.delab.util;

/**
 * An interface defines a method to produce random numbers, random primes, prime
 * greater or equal than a limit etc.
 *
 * @author Akis Papadopoulos
 */
public interface Generator {

    /**
     * A method generates numbers taking into account the limit.
     *
     * @param limit a limit bounding a range of possible numbers.
     * @return an integer number.
     */
    public int generate(int limit);
}
