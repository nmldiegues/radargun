package org.radargun.microbenchmark.domain;

import java.io.Serializable;

import org.radargun.CacheWrapper;


public class IntSetLinkedList implements IntSet, Serializable {

    public class Node implements Serializable {
	/* final */ private int m_value;

	public Node(CacheWrapper cache, int value, Node next) {
	    m_value = value;
	    setNext(cache, next);
	}

	public Node(CacheWrapper cache, int value) {
	    this(cache, value, null);
	}

	public int getValue() {
	    return m_value;
	}

	public void setNext(CacheWrapper cache, Node next) {
	    if (next != null) {
		Micro.put(cache, m_value + ":next", next);
	    }
	}

	public Node getNext(CacheWrapper cache) {
	    return (Node) Micro.get(cache, m_value + ":next");
	}
    }

    /* final */ private Node m_first;

    public IntSetLinkedList() { }

    public IntSetLinkedList(CacheWrapper cache) {
	Node min = new Node(cache, Integer.MIN_VALUE);
	Node max = new Node(cache, Integer.MAX_VALUE);
	min.setNext(cache, max);
	m_first = min;
    }

    public boolean add(CacheWrapper cache, int value) {
	boolean result;

	Node previous = m_first;
	Node next = previous.getNext(cache);
	int v;
	while ((v = next.getValue()) < value) {
	    previous = next;
	    next = previous.getNext(cache);
	}
	result = v != value;
	if (result) {
	    previous.setNext(cache, new Node(cache, value, next));
	}

	return result;
    }

    public boolean remove(CacheWrapper cache, int value) {
	boolean result;

	Node previous = m_first;
	Node next = previous.getNext(cache);
	int v;
	while ((v = next.getValue()) < value) {
	    previous = next;
	    next = previous.getNext(cache);
	}
	result = v == value;
	if (result) {
	    previous.setNext(cache, next.getNext(cache));
	}

	return result;
    }

    public boolean contains(CacheWrapper cache, int value) {
	boolean result;

	Node previous = m_first;
	Node next = previous.getNext(cache);
	int v;
	while ((v = next.getValue()) < value) {
	    previous = next;
	    next = previous.getNext(cache);
	}
	result = (v == value);

	return result;
    }
}
