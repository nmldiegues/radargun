package org.radargun.microbenchmark.domain;

import java.io.Serializable;
import java.util.Random;

import org.radargun.CacheWrapper;

public class IntSetSkipList implements IntSet, Serializable {

    public class Node implements Serializable {
	/* final */ private int m_value;
	/* final */ private int level;

	public Node(int level, int value) {
	    this.level = level;
	    this.m_value = value;
	}

	public void setForward(CacheWrapper cache, int index, Node node) {
	    Micro.put(cache, m_value + ":" + index + ":next", node);
	}

	public Node getForward(CacheWrapper cache, int index) {
	    return (Node) Micro.get(cache, m_value + ":" + index + ":next");
	}

	public int getValue() {
	    return m_value;
	}

	public int getLevel() {
	    return level;
	}

    }

    // Probability to increase level
    /* final */ private double m_probability = 0.25;

    // Upper bound on the number of levels
    /* final */ private int m_maxLevel = 32;

    // Highest level so far: level in cache

    // First element of the list
    /* final */ private Node m_head;
    // Thread-private PRNG
    /* final */ private static ThreadLocal<Random> s_random = new ThreadLocal<Random>() {
	protected synchronized Random initialValue() {
	    return new Random();
	}
    };

    private void setLevel(CacheWrapper cache, int level) {
	Micro.put(cache, "skipList:level", level);
    }

    private Integer getLevel(CacheWrapper cache) {
	return (Integer) Micro.get(cache, "skipList:level");
    }

    public IntSetSkipList() { }

    public IntSetSkipList(CacheWrapper cache) {
	setLevel(cache, 0);

	m_head = new Node(m_maxLevel, Integer.MIN_VALUE);
	Node tail = new Node(m_maxLevel, Integer.MAX_VALUE);
	for (int i = 0; i <= m_maxLevel; i++)
	    m_head.setForward(cache, i, tail);
    }

    protected int randomLevel() {
	int l = 0;
	while (l < m_maxLevel && s_random.get().nextDouble() < m_probability)
	    l++;
	return l;
    }

    public boolean add(CacheWrapper cache, int value) {
	boolean result;

	Node[] update = new Node[m_maxLevel + 1];
	Node node = m_head;

	for (int i = getLevel(cache); i >= 0; i--) {
	    Node next = node.getForward(cache, i);
	    while (next.getValue() < value) {
		node = next;
		next = node.getForward(cache, i);
	    }
	    update[i] = node;
	}
	node = node.getForward(cache, 0);

	if (node.getValue() == value) {
	    result = false;
	} else {
	    int level = randomLevel();
	    if (level > getLevel(cache)) {
		for (int i = getLevel(cache) + 1; i <= level; i++)
		    update[i] = m_head;
		setLevel(cache, level);
	    }
	    node = new Node(level, value);
	    for (int i = 0; i <= level; i++) {
		node.setForward(cache, i, update[i].getForward(cache, i));
		update[i].setForward(cache, i, node);
	    }
	    result = true;
	}


	return result;
    }

    public boolean remove(CacheWrapper cache, int value) {
	boolean result;

	Node[] update = new Node[m_maxLevel + 1];
	Node node = m_head;

	for (int i = getLevel(cache); i >= 0; i--) {
	    Node next = node.getForward(cache, i);
	    while (next.getValue() < value) {
		node = next;
		next = node.getForward(cache, i);
	    }
	    update[i] = node;
	}
	node = node.getForward(cache, 0);

	if (node.getValue() != value) {
	    result = false;
	} else {
	    int auxLimit = getLevel(cache);
	    for (int i = 0; i <= auxLimit; i++) {
		if (update[i].getForward(cache, i) == node)
		    update[i].setForward(cache, i, node.getForward(cache, i));
	    }
	    while (getLevel(cache) > 0 && m_head.getForward(cache, getLevel(cache)).getForward(cache, 0) == null)
		setLevel(cache, getLevel(cache) - 1);
	    result = true;
	}

	return result;
    }

    public boolean contains(CacheWrapper cache, int value) {
	boolean result;

	Node node = m_head;
	int initialM_Level = getLevel(cache);

	for (int i = initialM_Level; i >= 0; i--) {
	    Node next = node.getForward(cache, i);
	    while (next.getValue() < value) {
		node = next;
		next = node.getForward(cache, i);
	    }
	}
	node = node.getForward(cache, 0);

	result = (node.getValue() == value);

	return result;

    }

}
