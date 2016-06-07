package soxrecorderv2.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * copied from 
 * http://stackoverflow.com/questions/221525/how-would-you-implement-an-lru-cache-in-java
 * 
 * @author tomotaka
 *
 * @param <K>
 * @param <V>
 */
public class LRUCacheMap<K, V> extends LinkedHashMap<K, V> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final int maxEntries;
	
	public LRUCacheMap(final int maxEntries) {
		super(maxEntries + 1, 1.0f, true);
		this.maxEntries = maxEntries;
	}
	
	@Override
	protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
		return (maxEntries < super.size());
	}

}
