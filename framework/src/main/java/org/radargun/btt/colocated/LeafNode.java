package org.radargun.btt.colocated;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.radargun.LocatedKey;

public class LeafNode<T extends Serializable> extends AbstractNode<T> implements Serializable {

    private LocatedKey keyEntries;
    private LocatedKey keyPrevious;
    private LocatedKey keyNext;
    
    protected LeafNode(int group) {
	super(group);
	this.group = super.group;
	ensureKeys();
	setEntries(new DoubleArray<Serializable>(Serializable.class));
    }
    
    protected LeafNode(int group, DoubleArray<Serializable> entries) {
	super(group);
	this.group = super.group;
	ensureKeys();
	setEntries(entries);
    }
    
    protected LeafNode(LeafNode old, int newGroup) {
	super(newGroup);
	this.group = newGroup;
	// super.setParent(parent)	// set by the parent
	ensureKeys();
	setPrevious(old.getPrevious());
	setNext(old.getNext());
	setEntries(old.getEntries(false));
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LeafNode)) {
            return false;
        }
        LeafNode other = (LeafNode) obj;
        return this.keyEntries.equals(other.keyEntries) && this.group == other.group;
    }
    
    @Override
    void clean() {
        super.clean();
        BPlusTree.putCache(this.keyEntries, null);
        BPlusTree.putCache(this.keyPrevious, null);
        BPlusTree.putCache(this.keyNext, null);
    }
    
    private void ensureKeys() {
	if (this.keyEntries == null) {
	    this.keyEntries = BPlusTree.wrapper.createGroupingKey("entries-" + UUID.randomUUID().toString(), this.group);
	}
	if (this.keyPrevious == null) {
	    this.keyPrevious = BPlusTree.wrapper.createGroupingKey("previous-" + UUID.randomUUID().toString(), this.group);
	}
	if (this.keyNext == null) {
	    this.keyNext = BPlusTree.wrapper.createGroupingKey("next-" + UUID.randomUUID().toString(), this.group);
	}
    }
    
    protected DoubleArray<Serializable> getEntries(boolean ghostRead) {
	if (ghostRead) {
	    return (DoubleArray<Serializable>) BPlusTree.cacheGetShadow(keyEntries);
	} else {
	    return (DoubleArray<Serializable>) BPlusTree.getCache(keyEntries);
	}
    }
    
    protected void setEntries(DoubleArray<Serializable> entries) {
	BPlusTree.putCache(keyEntries, entries);
    }
    
    protected LeafNode<T> getPrevious() {
	return (LeafNode<T>) BPlusTree.getCache(keyPrevious);
    }
    
    protected void setPrevious(LeafNode<T> previous) {
	BPlusTree.putCache(keyPrevious, previous);
    }
    
    protected LeafNode<T> getNext() {
	return (LeafNode<T>) BPlusTree.getCache(keyNext);
    }
    
    protected void setNext(LeafNode<T> next) {
	BPlusTree.putCache(keyNext, next);
    }
    
    
    @Override
    public AbstractNode insert(boolean remote, Comparable key, Serializable value, int height, String localRootsUUID, LocatedKey cutoffKey) {
	DoubleArray<Serializable> localEntries = this.getEntries(false);
	DoubleArray<Serializable> localArr = justInsert(localEntries, key, value);

	if (localArr == null) {
	    return null;
	}
	if (localArr.length() <= BPlusTree.MAX_NUMBER_OF_ELEMENTS) {
	    return getRoot();
	} else {
	    // find middle position
	    Comparable keyToSplit = localArr.findRightMiddlePosition();

	    // split node in two
	    int newGroup = this.group;
	    LeafNode leftNode = new LeafNode<T>(newGroup, localArr.leftPart(BPlusTree.LOWER_BOUND + 1));
	    LeafNode rightNode = new LeafNode<T>(newGroup, localArr.rightPart(BPlusTree.LOWER_BOUND + 1));
	    fixLeafNodeArraysListAfterSplit(leftNode, rightNode);

	    InnerNode parent = this.getParent(true);
	    this.clean();

	    // propagate split to parent
	    if (parent == null) {  // make new root node
		InnerNode newRoot = new InnerNode<T>(newGroup, leftNode, rightNode, keyToSplit);
		return newRoot;
	    } else {
		return parent.rebase(leftNode, rightNode, keyToSplit, height, 1, localRootsUUID, cutoffKey, BPlusTree.getCutoff(cutoffKey));
	    }
	}
    }
    
    private DoubleArray<Serializable> justInsert(DoubleArray<Serializable> localEntries, Comparable key, Serializable value) {
        // this test is performed because we need to return a new structure in
        // case an update occurs.  Value types must be immutable.
	Serializable currentValue;
	try {
	    currentValue = localEntries.get(key);
	} catch (NullPointerException npe) {
	    npe.printStackTrace();
	    throw npe;
	}
        // this check suffices because we do not allow null values
        if (currentValue != null && currentValue.equals(value)) {
            return null;
        } else {
            DoubleArray<Serializable> newArr = localEntries.addKeyValue(key, value);
            setEntries(newArr);
            return newArr;
        }
    }

    private void fixLeafNodeArraysListAfterSplit(LeafNode leftNode, LeafNode rightNode) {
	LeafNode myPrevious = this.getPrevious();
	LeafNode myNext = this.getNext();
	
	if (myPrevious != null) {
	    leftNode.setPrevious(myPrevious);
	}
        rightNode.setPrevious(leftNode);
        if (myNext != null) {
            rightNode.setNext(myNext);
        }
        leftNode.setNext(rightNode);
        
        if (myPrevious != null) {
            myPrevious.setNext(leftNode);
        }
        if (myNext != null) {
            myNext.setPrevious(rightNode);
        }
    }

    @Override
    public AbstractNode remove(boolean remote, Comparable key, int height, String localRootsUUID, LocatedKey cutoffKey) {
	DoubleArray<Serializable> localEntries = this.getEntries(false);
	DoubleArray<Serializable> localArr = justRemove(localEntries, key);
	
        if (localArr == null) {
            return null;	// remove will return false
        }
        if (getParent(false) == null) {
            return this;
        } else {
            // if the removed key was the first we need to replace it in some parent's index
            Comparable replacementKey = getReplacementKeyIfNeeded(key);

            if (localArr.length() < BPlusTree.LOWER_BOUND) {
        	Integer cutoff = BPlusTree.getCutoff(cutoffKey);
                return getParent(false).underflowFromLeaf(key, replacementKey, height, 0, localRootsUUID, cutoff);
            } else if (replacementKey != null) {
                return getParent(false).replaceDeletedKey(key, replacementKey);
            } else {
                return getParent(false).getRoot(); // maybe a tiny faster than just getRoot() ?!
            }
        }
    }

    private DoubleArray<Serializable> justRemove(DoubleArray<Serializable> localEntries, Comparable key) {
        // this test is performed because we need to return a new structure in
        // case an update occurs.  Value types must be immutable.
        if (!localEntries.containsKey(key)) {
            return null;
        } else {
            DoubleArray<Serializable> newArr = localEntries.removeKey(key);
            setEntries(newArr);
            return newArr;
        }
    }

    // This method assumes that there is at least one more key (which is
    // always true if this is not the root node)
    private Comparable getReplacementKeyIfNeeded(Comparable deletedKey) {
        Comparable firstKey = this.getEntries(false).firstKey();
        if (BPlusTree.COMPARATOR_SUPPORTING_LAST_KEY.compare(deletedKey, firstKey) < 0) {
            return firstKey;
        } else {
            return null; // null means that key does not need replacement
        }
    }

    @Override
    DoubleArray<Serializable>.KeyVal removeBiggestKeyValue() {
        DoubleArray<Serializable> entries = this.getEntries(false);
        DoubleArray<Serializable>.KeyVal lastEntry = entries.getBiggestKeyValue();
        setEntries(entries.removeBiggestKeyValue());
        return lastEntry;
    }

    @Override
    DoubleArray<Serializable>.KeyVal removeSmallestKeyValue() {
        DoubleArray<Serializable> entries = this.getEntries(false);
        DoubleArray<Serializable>.KeyVal firstEntry = entries.getSmallestKeyValue();
        setEntries(entries.removeSmallestKeyValue());
        return firstEntry;
    }

    @Override
    Comparable getSmallestKey() {
        return this.getEntries(false).firstKey();
    }

    @Override
    void addKeyValue(DoubleArray.KeyVal keyValue) {
        setEntries(this.getEntries(false).addKeyValue(keyValue));
    }

    @Override
    void mergeWithLeftNode(AbstractNode leftNode, Comparable splitKey, int treeDepth, int height, String localRootsUUID, int cutoff) {
        LeafNode left = (LeafNode) leftNode; // this node does not know how to merge with another kind
        setEntries(getEntries(false).mergeWith(left.getEntries(false)));

        LeafNode nodeBefore = left.getPrevious();
        if (nodeBefore != null) {
            this.setPrevious(nodeBefore);
            nodeBefore.setNext(this);
        }

        // no need to update parents, because they are always the same for the two merging leaf nodes
        assert (this.getParent(false).equals(leftNode.getParent(false)));
        
        left.clean();
    }

    @Override
    public T get(Comparable key) {
	DoubleArray<Serializable> localEntries = this.getEntries(false);
	return (T) localEntries.get(key);
    }

    @Override
    public boolean containsKey(boolean remote, Comparable key) {
	DoubleArray<Serializable> localEntries = this.getEntries(false);
	try {
	    return localEntries.containsKey(key);
	} catch (NullPointerException npe) {
	    npe.printStackTrace();
	    throw npe;
	}
    }

    @Override
    int shallowSize() {
        return this.getEntries(false).length();
    }

    @Override
    public int size() {
        return this.getEntries(false).length();
    }

    @Override
    public Iterator<T> iterator() {
        return new LeafNodeArrayIterator(this);
    }

    private class LeafNodeArrayIterator implements Iterator<T> {
        private int index;
        private Serializable[] values;
        private LeafNode current;

        LeafNodeArrayIterator(LeafNode LeafNodeArray) {
            this.index = 0;
            this.values = LeafNodeArray.getEntries(false).values;
            this.current = LeafNodeArray;
        }

        @Override
        public boolean hasNext() {
            if (index < values.length) {
                return true;
            } else {
                return this.current.getNext() != null;
            }
        }

        @Override
        public T next() {
            if (index >= values.length) {
                LeafNode nextNode = this.current.getNext();
                if (nextNode != null) {
                    this.current = nextNode;
                    this.index = 0;
                    this.values = this.current.getEntries(false).values;
                } else {
                    throw new NoSuchElementException();
                }
            }
            index++;
            return (T) values[index - 1];
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("This implementation does not allow element removal via the iterator");
        }

    }

    @Override
    public String dump(int level, boolean dumpKeysOnly, boolean dumpNodeIds) {
        StringBuilder str = new StringBuilder();
        str.append(BPlusTree.spaces(level));
        if (dumpNodeIds) {
            str.append(this.getPrevious() + "<-[" + this + ": ");
        } else {
            str.append("[: ");
        }

        DoubleArray<Serializable> subNodes = this.getEntries(false);
        for (int i = 0; i < subNodes.length(); i++) {
            Comparable key = subNodes.keys[i];
            Serializable value = subNodes.values[i];
            str.append("(" + value);
            str.append(dumpKeysOnly ? ") " : "," + key + ") ");
        }
        if (dumpNodeIds) {
            str.append("]->" + this.getNext() + " ^" + getParent(false) + "\n");
        } else {
            str.append("]\n");
        }

        return str.toString();
    }

    @Override
    public AbstractNode changeGroup(int newGroup) {
	return new LeafNode(this, newGroup);
    }

}
