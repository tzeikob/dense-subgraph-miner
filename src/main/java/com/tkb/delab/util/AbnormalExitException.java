package com.tkb.delab.util;

/**
 * A exception for abnormal exit.
 *
 * @author Akis Papadopoulos
 */
public class AbnormalExitException extends Exception {

    /**
     * A default constructor.
     *
     * @param message a short message describing the exception.
     */
    public AbnormalExitException(String message) {
        super(message);
    }
}
