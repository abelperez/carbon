/**
 * Copyright (C) 2011 Mindplex Media, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.mindplex.util;

import java.util.*;

/**
 * A weighted round robin list for efficient load balancing of elements
 * contained in this {@code WeightedRoundRobinList}.
 * 
 * @author Abel Perez
 */
public class WeightedRoundRobinList<E> implements LoadBalancedList<E>, Iterable<E> {
    /**
     * The backing list of elements contained in this
     * {@code WeightedRoundRobinList}.
     */
    private List<Element<E>> elements = new ArrayList<Element<E>>();

    /**
     * The position this list is currently in.  Each time this list is
     * accessed via the {@code get} method the position is tracked.
     * When this list has provided a complete distribution of its weighted
     * elements, the position is reset.
     *
     * <p>Invocations of the {@code next} method of this lists {@link Iterator},
     * also cause the position to adjust.
     */
    private int position;

    /**
     * This lists modification count. Each time this list is modified, this
     * counter is incremented.  This helps iterators of this list detect
     * concurrent modifications.
     */
    private int modCount = 0;

    /**
     * Constructs an empty {@code WeightedRoundRobinList}.
     */
    public WeightedRoundRobinList() {
    }

    /**
     * Constructs this {@code WeightedRoundRobinList} with the specified
     * collection of elements.
     * 
     * @param elements the collection of elements to initialize this list with.
     */
    public WeightedRoundRobinList(List<Element<E>> elements) {
        loadElements(Check.forNull(elements));
    }

    /**
     * Adds the specified {@code value} and it's corresponding {@code weight}
     * to this list. A weight with a value equal to or less than zero will
     * prevent the specified value from being added to this list.  Valid weight
     * values must be greater than zero.
     *
     * <p>If the specified {@code value} and {@code weight} combination exist
     * in this list then only the weight for the given value is updated;
     * otherwise the value weight combination is added to this list.
     *
     * @param value the value to add to this list.
     * @param weight the weight to apply to the specified value.
     *
     * @return LoadBalanceList this list.
     */
    public LoadBalancedList add(E value, int weight) {

        // if the specified weight is less than zero
        // there's no need to proceed.  Valid weight
        // values must be greater than zero.
        if (weight < 0) return this;

        // In order to verify if the specified value
        // and weight combination already exist in this
        // list, we create an element node that we can
        // use to search this list.
        Element<E> element = new Element<E>(value, weight);

        int index = elements.indexOf(element);

        // if this list does not contain the given value
        // weight combination we add it to this list;
        // otherwise we update the existing weight of the
        // specified value with the given weight.

        if (index < 0) {
            elements.add(element);

        } else {
            Element<E> target = elements.get(index);
            target.setWeight(weight);

            // if the specified weight is smaller than the
            // values distribution count, we reset the count.
            // In other words this means that the value
            // has been accessed more times than the new
            // weight allows.
            if (target.getCount() > target.getWeight()) {
                target.setCount(0);
            }
        }

        modCount++;
        return this;
    }

    /**
     * Sets the specified {@code value} and it's corresponding {@code weight}
     * to this list. A weight with a value equal to or less than zero will
     * prevent the specified value from being added to this list.  Valid weight
     * values must be greater than zero.
     *
     * <p>If the specified {@code value} and {@code weight} combination exist
     * in this list then only the weight for the given value is updated;
     * otherwise the value weight combination is added to this list.
     *
     * @param value the value to add to this list.
     * @param weight the weight to apply to the specified value.
     *
     * @return LoadBalanceList this list.
     */
    public LoadBalancedList set(E value, int weight) {
        return add(value, weight);
    }
    
    /**
     * Removes the specified {@code element} from this list. If this list does
     * not contain the specified element, then no action is taken and this
     * method returns <tt>false</tt>; otherwise <tt>true</true>.
     * 
     * @param element the element to remove from this list.
     *
     * @return {@code true} if this {@code WeightedRoundRobinList} is modified,
     * {@code false} otherwise.
     */
    public boolean remove(E element) {
        Element<E> target = new Element<E>(element, 0);
        boolean changed = elements.remove(target);

        if (changed) modCount++;
        return changed;
    }

    /**
     * Removes the specified collection of elements from this list. If this
     * list does not contain any of the specified elements, then no action is
     * taken and this method returns <tt>false</tt>; otherwise <tt>true</true>.
     *
     * @param collection the collection of elements to remove from this list.
     *
     * @return {@code true} if this {@code WeightedRoundRobinList} is modified;
     * otherwise {@code false}.
     */
    public boolean removeAll(Collection<E> collection) {
        for (E element : collection) {
            if (! remove(element)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Removes all the elements in this list that are not contained in the
     * specified {@code collection}.  Once this method is complete, this
     * list will only contain the elements found in the specified
     * {@code collection}.
     *  
     * @param collection the collection of elements to retain in this list.
     *
     * @return {@code true} if this {@code WeightedRoundRobinList} is modified;
     * otherwise {@code false}.
     */
    public boolean retainAll(Collection<E> collection) {

        boolean changed = false;

        for (int i = 0; i < elements.size(); i++) {
            Element<E> target = elements.get(i);

            // if the current element is not contained in
            // the specified collection, we remove it from
            // this list; otherwise we retain it.
            if (! collection.contains(target.getValue())) {
                remove(target.getValue());
                changed = true;
            }
        }

        if (changed) modCount++;
        return changed;
    }

    /**
     * Gets the next available item in this list. The item provided is
     * determined by its weight.  This list will provide elements in a
     * weighted round robin fashion.  Distributing the elements of this
     * list in weighted round robin fashion allows for efficiency in
     * load balancing the distribution of elements in this list.
     *
     * <p>For example, the following code illustrates how this list effectively
     * load balances the distribution of elements contained in this list.
     *
     * <pre>
     * {@code
     * LoadBalanceList<String> list = new WeightedRoundRobinList<String>();
     * list.add("low", 1);
     * list.add("mid", 2);
     * list.add("high", 3);
     *
     * for (String element : list) {
     *     System.out.println(item + " ");
     * }}
     * </pre>
     *
     * <p>will produce the following output:
     * <em>low mid high mid high high </em>
     *
     * <p>As you can see the code above yields each element the number
     * of times defined by its weight for each complete iteration of this
     * list.
     *
     * <p>Its important to note that each call to this lists {@code iterator}
     * method, resets the current position of this list. Calling {@code get}
     * several times, then calling the {@code iterator} method, resets this
     * list to a state that is equal to this lists {@code get} method never
     * being called.
     * 
     * @return
     */
    public E get() {

        if (elements.size() == 0) return null;

        if (isDistributionComplete())  {
            resetDistributionCounts();
            position = 0;
        }

        boolean found = false;
        while (! found) {

            // if the current position exceeds the
            // size of the elements, then we reset the current
            // position to zero.  This effectively means that we
            // have reached the end of the list, as a result of
            // calling get.
            if (position >= elements.size()) {
                position = 0;
            }

            Element node = elements.get(position);

            // if the distribution count of the current
            // element is less than it's weight, then we
            // know that this element can be accessed.
            // We increment the elements distribution
            // count and break out of the loop.
            
            if (node.getCount() < node.getWeight()) {
                node.incrementCount();
                found = true;

            } else {
                // this element is not the one we want
                // so we move the position forward and
                // continue through the loop.
                position++;
            }
        }

        // get the element at the current position and
        // increment the modified count.
        Element<E> result = elements.get(position++);
        // modCount++;
        return result.getValue();
    }

    /**
     * Returns {@code true} if the specified element is contained within this
     * list; otherwise {@code false}.
     * 
     * @param element the element to search for in this list.
     *
     * @return {@code true} if the specified element is contained within this
     * list; otherwise {@code false}.
     */
    public boolean contains(E element) {
        Element<E> target = new Element<E>(element, 0);
        return elements.contains(target);
    }

    /**
     * Returns {@code true} if the specified collection of element is contained
     * within this list; otherwise {@code false}.
     *
     * @param collection the collection of elements to search for in this list.
     *
     * @return {@code true} if the specified collection of elements is contained
     * within this list; otherwise {@code false}.
     */
    public boolean containsAll(Collection<E> collection) {
        for (E element : collection) {

            // no need to continue if at least
            // one element from the specified
            // collection is not contained in
            // this list.
            if (! contains(element)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns <tt>true</tt> if this {@code WeightedRoundRobinList} contains
     * elements; otherwise <tt>false</tt>.
     *
     * @return <tt>true</tt> if this {@code WeightedRoundRobinList} contains
     * elements; otherwise <tt>false</tt>.
     */
    public boolean isEmpty() {
        return elements.isEmpty();
    }

    /**
     * Returns the count of elements contained in this list.
     * 
     * @return the count of elements contained in this list.
     */
    public int size() {
        return elements.size();
    }

    /**
     * Removes all the elements contained in this
     * {@code WeightedRoundRobinList}.
     */
    public void clear() {
        elements.clear();
        position = 0;
    }

    /**
     * Gets the list of elements contained in this {@code WeightedRoundRobinList}.
     *
     * @return the list of elements contained in this
     * {@code WeightedRoundRobinList}.
     */
    public List<Element<E>> elements() {
        return Collections.unmodifiableList(this.elements);
    }

    /**
     * Gets an instance of {@code Iterator} that provides access to the elements
     * contained in this {@code WeightedRoundRobinList}.  This method resets
     * this lists current position and returns the elements of this list in the
     * same order that calling {@code get} would return.  In other words this
     * iterator preserves the weighted round robin characteristics of this list.
     *  
     * @return an {@code Iterator} for accessing the elements contained in this
     * list.
     */
    public Iterator<E> iterator() {
        resetDistributionCounts();
        return new WeightedRoundRobinIterator();
    }
    
    /**
     * This {@code WeightedRoundRobinList} concludes equality with the given
     * object by comparing each element in the specified list. The order of
     * elements also determines if the given object is equal to this list.
     * Lastly, the specified object's size must match this lists size in order
     * for both objects to be equal.
     *
     * @param other the other list to compare to this list for equality.
     *
     * @return {@code true} if the specified object is equal to this list.
     */
    @Override public boolean equals(Object other) {

        if (this == other) {
            return true;
        }

        if (other instanceof LoadBalancedList) {

            WeightedRoundRobinList<?> otherList = (WeightedRoundRobinList<?>) other;

            // the size of the specified object must match
            // the size of this list.
            if (otherList.size() != size()) {
                return false;
            }

            Iterator<?> one = elements.iterator();
            Iterator<?> two = otherList.elements.iterator();

            // the order of the elements in the specified
            // object must match the order of elements in
            // this list.
            while(one.hasNext()) {
                Object e1 = one.next();
                Object e2 = two.next();

                if (! (e1 == null ? e2 == null : e1.equals(e2))) {
                    return false;
                }
            }

            return true;
        }

        return true;
    }

    /**
     * Gets the hash code for this list.  Each element in this list is
     * used to conclude the final hash code for this list.
     * 
     * @return this lists hash code.
     */
    @Override public int hashCode() {
        int hash = 1;

        for (Element<E> element : elements) {
            hash = (31 * hash) + (element == null ? 0 : element.hashCode());
        }

        return hash;
    }

    /**
     * Loads the specified list of elements into this
     * {@code WeightedRoundRobinList}.  Any existing elements in this list
     * are removed before loading the specified list.  In other words this
     * method re-initializes this list.
     * 
     * @param elements the list of elements to reinitialize this list with.
     */
    protected void loadElements(List<Element<E>> elements) {

        // this method is basically a way to
        // reinitialize this list, so we clear
        // any existing elements in this list.
        elements.clear();

        for (Element<E> element : elements) {
            this.elements.add(element);
        }

        modCount++;
    }

    /**
     * Returns {@code true} if every element in this list has been
     * distributed according to its weight.  For example, this method
     * return tue if a list with an element A that has a weight of 1
     * and a second element B with a weight of 2 has been accessed in
     * the following order: A, B, B.
     * 
     * @return {@code true} if every element in this list has been
     * distributed according to its weight.
     */
    protected boolean isDistributionComplete() {
        boolean complete = true;

        for (Element element : elements) {
            if (element.getCount() < element.getWeight()) {
                complete = false;
            }
        }
        return complete;
    }

    /**
     * Resets the distribution count for each element in this list to {@code 0}.
     */
    protected void resetDistributionCounts() {
        for (Element element : elements) {
            element.setCount(0);
        }

        // Since we are resetting the distribution count,
        // we also reset the modification count.  
        modCount = 0;
    }

    /**
     * A weighted round robin iterator that provides access to the elements
     * contained in the list this iterator represents.  The order in which
     * elements are provided through the {@code next} method is based on the
     * weighted round robin algorithm implemented by
     * {@code WeightedRoundRobinList}.
     */
    private class WeightedRoundRobinIterator implements Iterator<E>
    {
        /**
         * The expected count is a snapshot of the current modified count
         * of the list this iterator represents.  This count helps this
         * iterator detect any concurrent modifications made to the list.
         */
        private int expectedModCount = modCount;

        /**
         * Constructs this {@code WeightedRoundRobinIterator}.
         */
        public WeightedRoundRobinIterator() {
        }

        /**
         *
         * @return
         */
        public boolean hasNext() {
            // check if there is any elements to iterate
            // over and that we have not completed iterating
            // over all the elements contained in the list
            // this iterator represents.
            return !elements.isEmpty() && !isDistributionComplete();
        }

        /**
         * 
         * @return
         */
        public E next() {

            // blow up if we detect that the list
            // this iterator represents has been
            // modified.
            if (expectedModCount != modCount) {
                throw new ConcurrentModificationException();
            }

            // blow up with a no such element exception
            // because we have completely iterated over
            // all the elements in the list this iterator
            // represents.
            if (isDistributionComplete()) {
                throw new NoSuchElementException();
            }

            try {
                return get();
                
            } catch (IndexOutOfBoundsException exception) {
                throw new NoSuchElementException();
            }
        }

        /**
         * This operation is not supported.  This iterator is
         * not a concurrent iterator.
         */
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}