package org.radargun.stamp.vacation.domain;

import java.io.Serializable;

import org.radargun.CacheWrapper;
import org.radargun.stamp.vacation.Vacation;


public class RBTree<K extends Comparable<K>, V> implements Serializable {

    /* final */ protected String cacheKey;

    public RBTree() {
	
    }
    
    @SuppressWarnings("unchecked")
    public RBTree(CacheWrapper cache, String cacheKey) {
	this.cacheKey = cacheKey;
	Vacation.put(cache, cacheKey, new RedBlackTree<Entry<K, V>>(true));
    }
    
    private RedBlackTree<Entry<K, V>> getIndex(CacheWrapper cache) {
	RedBlackTree<Entry<K, V>> v = (RedBlackTree<Entry<K, V>>)Vacation.get(cache, cacheKey);
	return v;
    }
    
    private void putIndex(CacheWrapper cache, RedBlackTree<Entry<K, V>> index) {
	Vacation.put(cache, cacheKey, index);
    }

    public void put(CacheWrapper cache, K key, V value) {
	if (value == null) {
	    throw new RuntimeException("RBTree does not support null values!");
	}
	putIndex(cache, getIndex(cache).put(new Entry<K, V>(key, value)));
    }

    public V putIfAbsent(CacheWrapper cache, K key, V value) {
	if (value == null) {
	    throw new RuntimeException("RBTree does not support null values!");
	}

	Entry<K, V> newEntry = new Entry<K, V>(key, value);
	Entry<K, V> oldVal = getIndex(cache).get(newEntry);
	if (oldVal != null) {
	    return oldVal.value;
	}

	putIndex(cache, getIndex(cache).put(newEntry));
	return null;
    }

    public V get(CacheWrapper cache, K key) {
	Entry<K, V> entry = new Entry<K, V>(key, null);
	Entry<K, V> oldVal = getIndex(cache).get(entry);
	if (oldVal != null) {
	    return oldVal.value;
	} else {
	    return null;
	}
    }

    public boolean remove(CacheWrapper cache, K key) {
	Entry<K, V> entry = new Entry<K, V>(key, null);
	Entry<K, V> existing = getIndex(cache).get(entry);
	putIndex(cache, getIndex(cache).put(entry));
	return (existing.value != null);
    }

    static class Entry<K extends Comparable<K>, V> implements Comparable<Entry<K, V>>, Serializable {
	private final K key;
	private final V value;

	Entry(K key, V value) {
	    this.key = key;
	    this.value = value;
	}

	@Override
	public int compareTo(Entry<K, V> other) {
	    return this.key.compareTo(other.key);
	}
    }

}
