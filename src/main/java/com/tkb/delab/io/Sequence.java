package com.tkb.delab.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.io.Writable;

/**
 * A writable comparable representing an ordered sequence of integers.
 *
 * @author Akis Papadopoulos
 */
public class Sequence<E extends Writable> extends ArrayList<E> implements Writable {

    private static final long serialVersionUID = 4911321393319821791L;

    /**
     * A constructor creating a sequence of integers.
     */
    public Sequence() {
    }

    /**
     * A constructor creating a sequence of integers.
     *
     * @param items the sequence of integers.
     */
    public Sequence(ArrayList<E> items) {
        super(items);
    }

    /**
     * A constructor creating a sequence of integers.
     *
     * @param items the sequence of integers.
     */
    public Sequence(E... items) {
        super(Arrays.asList(items));
    }

    /**
     * A method deserializing this sequence.
     *
     * @param in source for raw byte representation.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        // Clearing the list
        this.clear();

        // Getting the number of items
        int numFields = in.readInt();

        // Canceling in case of no items
        if (numFields == 0) {
            return;
        }

        try {
            // Getting the name of the class
            String className = in.readUTF();

            // Loading an instance of the class
            Class<E> c = (Class<E>) Class.forName(className);

            // Creating an reference of the class
            E obj;

            // Iterating through the items
            for (int i = 0; i < numFields; i++) {
                // Creating a new instance of the class
                obj = (E) c.newInstance();

                // Reading the next item
                obj.readFields(in);

                // Adding the item into the sequence
                this.add(obj);
            }

        } catch (Exception exc) {
        }
    }

    /**
     * A method serializing this sequence.
     *
     * @param out where to write the raw byte representation.
     */
    @Override
    public void write(DataOutput out) throws IOException {
        // Writing the number of items
        out.writeInt(this.size());

        // Canceling in case of no items
        if (size() == 0) {
            return;
        }

        // Getting the first item
        E obj = get(0);

        // Writing the name of the class
        out.writeUTF(obj.getClass().getCanonicalName());

        // Iterating through the items
        for (int i = 0; i < size(); i++) {
            // Gettign the next item
            obj = get(i);

            // Throwing an exception in case of a nullable item
            if (obj == null) {
                throw new IOException("Cannot serialize null fields!");
            }

            // Writing the item into the sequence
            obj.write(out);
        }
    }

    /**
     * A method generating a human-readable textual representation of this
     * sequence.
     *
     * @return human-readable textual representation of this sequence.
     */
    @Override
    public String toString() {
        // Creating an empty string builder
        StringBuilder sb = new StringBuilder();

        // Iterating through the items
        for (int i = 0; i < this.size(); i++) {
            // Appending the next item
            sb.append(this.get(i));

            // Separating the items by comma
            if (i < this.size() - 1) {
                sb.append(",");
            }
        }

        return sb.toString();
    }
}