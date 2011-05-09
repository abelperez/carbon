package com.mindplex.util;

/**
 *
 * @author Abel Perez
 */
public class Element<E>
{
    private E value;
    
    private int weight;
    
    private int count;

    public Element(E value, int weight) {
        this.value = value;
        this.weight = weight;
        this.count = 0;
    }

    public E getValue() {
        return value;
    }

    public void setValue(E value) {
        this.value = value;
    }
    
    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void incrementCount() {
        this.count = count + 1;
    }
    
    public void decrementCount() {
        this.count = count - 1;
    }

    @Override public boolean equals(Object other) {
        if (! (other instanceof Element)) {
            return false;
        }
        Element otherElement = (Element)other;
        return this.value.equals(otherElement.value);
    }

    @Override public int hashCode() {
        int hash = 7;
        hash = 97 * hash + (this.value != null ? this.value.hashCode() : 0);
        return hash;
    }
}