package org.radargun.cachewrappers;

import java.io.Serializable;

import org.infinispan.distribution.group.Group;
import org.radargun.LocatedKey;

public class GroupingKey implements LocatedKey, Serializable {

    private final String key;
    private final int group;
    
    public GroupingKey(String key, int group) {
	this.key = key;
	this.group = group;
    }
    
    @Override
    public int hashCode() {
        return key.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof GroupingKey)) {
            return false;
        }
        GroupingKey other = (GroupingKey) obj;
        return this.key.equals(other.key) && this.group == other.group;
    }
    
    public int getGroup() {
	return this.group;
    }
    
    public String getKey() {
	return this.key;
    }
    
    public String getShortKey() {
	int firstHyphen = this.key.indexOf("-");
	return key.substring(0, firstHyphen);
    }
    
    @Group
    public String group() {
	return "" + group;
    }
}
