/*
 * Created on 24.02.2004
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package mondrian.rolap.cache;


/**
 * An object must implement <code>Cacheable</code> to be stored in a cache.
 * 
 * <p>Methods <em>must not</em> be synchronized. To prevent the Cachepool calling
 * any of these methods, synchronize on Cachepool.instance(), e.g.
 * <pre>
 * synchronized(CachePool.instance())
 *   // Cacheable methods will not be called here
 * }
 * </pre>
 */
public interface Cacheable
{
	/** Removes the object from its cache. The {@link CachePool} calls this
	 * method when an object's cost exceeds its benefit. **/
	void removeFromCache();
	/** Sets this object's last-access time. Since the weight increases
	 * exponentially over time, less recently accessed objects decline in
	 * importance. **/
	void markAccessed(double recency);
	/** Calculates how valuable this object is in the cache. A larger
	 * number means is it more valuable, so it is less likely to be
	 * flushed. A typical formula would divide its <em>benefit</em> (the
	 * time it would take to re-create it in the cache, or not use it at
	 * all), by its <em>cost</em> (the number of bytes it occupies), and
	 * multiply by its <em>recency</em> value. It is important that all
	 * objects in the <code>CachePool</code> have comparable weights. **/
	double getScore();
	/** Returns the cost of this object. **/
	double getCost();
	/** Stores the object's pin count. Called from {@link
	 * CachePool#pin}. **/
	void setPinCount(int pinCount);
	/** Returns the object's pin count. If an object's pin count is greater
	 * than zero, it is not a candidate for being removed from the
	 * cache. **/
	int getPinCount();
}