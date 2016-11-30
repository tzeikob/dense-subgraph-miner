package com.tkb.delab.util;

/**
 * A prime number generator produces large prime numbers greater or equal to a
 * given limit, bounded down.
 *
 * @author Akis Papadopoulos
 */
public class PrimeGenerator implements Generator {

    /**
     * A method generates a prime number greater or equal to a given limit.
     *
     * @param limit a limit bounding down a range of possible numbers.
     * @return an integer prime number greater or equal than limit.
     */
    @Override
    public int generate(int limit) {
        //Iterating throug the candicates prime numbers
        for (int num = limit; true; num++) {
            //Checking if this number is prime
            if (isPrime(num)) {
                return num;
            }
        }
    }

    /**
     * A method to check if a given number is prime or not.
     *
     * @param num number to be checked if is prime.
     * @return a boolean value determines if number is prime.
     */
    private boolean isPrime(int num) {
        //Setting firstly as prime number
        boolean prime = true;

        //Limiting the checks needed
        int limit = (int) Math.sqrt(num);

        //Iterating through the divisors
        for (int i = 2; i <= limit; i++) {
            //Checking if number divised by the next divisor
            if (num % i == 0) {
                //Setting the number as not prime
                prime = false;

                //Breaking the process
                break;
            }
        }

        return prime;
    }
}
