package mondrian.rolap.cache;

import java.io.PrintWriter;
import java.util.Collection;

import junit.framework.Test;
import mondrian.olap.MondrianProperties;

/**
 * A <code>CachePool</code> manages the objects in a collection of
 * caches.
 * 
 * @author av
 */
public abstract class CachePool {

	/** Singleton. **/
	private static CachePool instance;

	/** Returns or creates the singleton. **/
	public static synchronized CachePool instance() {
		if (instance == null) {
			final int costLimit =
					MondrianProperties.instance().getCachePoolCostLimit();
			//instance = new MondrianCachePool(costLimit);
			instance = new SoftCachePool();
			//instance = new HardCachePool();
		}
		return instance;
	}

	/**
	 * returns an id that uniquely identifies the <code>cacheable</code>
	 */
	public static Object cacheableId(Cacheable cacheable) {
		return new Integer(System.identityHashCode(cacheable));
	}

	/**
	 * Records that an object has just been added to a cache.
	 **/
	public abstract void register(Cacheable cacheable);
	
	/**
	 * Records that an object has just been added to a cache, and pins it, in
	 * an atomic operation.
	 **/
	public abstract void register(Cacheable cacheable, int pinCount, Collection pinned);
	
	/**
	 * A {@link Cacheable} <em>must</em> call this from its
	 * <code>finalize</code> method.
	 */
	public abstract void deregister(Cacheable cacheable, boolean fromFinalizer);
	
	/**
	 * Records that an object has been accessed.
	 **/
	public abstract void access(Cacheable cacheable);
	
	/**
	 * Notifies the cache that an object's score has changed, and therefore it
	 * may need to be re-positioned in the priority queue.
	 **/
	public abstract void notify(Cacheable cacheable, double previousCost);
	
	/**
	 * Flushes all unpinned objects in the cache. Objects which are still
	 * pinned are marked, so they will be removed immediately that they are
	 * unpinned.
	 */
	public abstract void flush();
	
	/**
	 * Pins an object in its cache.
	 *
	 * <p>A client calls a method to ensure that important objects are present
	 * until an algorithm has completed. Clients should generally clean up pins
	 * after they complete using a finally block:<blockquote>
	 *
	 * <pre>ArrayList pinned = new ArrayList();
	 * try {
	 *   cachePool.pin(objectA, pinned);
	 *   cachePool.pin(objectB, pinned);
	 *   ...
	 * } finally {
	 *   cachePool.unpin(list);
	 * }</pre></blockquote>
	 *
	 * <p>Since several clients can be interested in the same object, this
	 * method increments a <dfn>pin count</dfn>. {@link
	 * #unpin(Cache.Cacheable)} decrements the pin count.
	 *
	 * <p>The object is not pinned if it is already in the
	 * <code>newlyPinned</code> list.
	 *
	 * @param cacheable Object to pin
	 * @param newlyPinned Collection of objects which have been pinned.
	 *   <code>cacheable</code> is added to this list if this method pins
	 *   it
	 **/
	public abstract void pin(Cacheable cacheable, Collection newlyPinned);

	/**
	 * Unpins an object from the cache. See {@link #pin}.
	 **/
	public abstract void unpin(Cacheable cacheable);
	
	/**
	 * Unpins all elements in a list. All elements must implement {@link
	 * Cacheable}. See {@link #unpin(Cacheable)}.
	 **/
	public abstract void unpin(Collection pinned);
	
	/**
	 * Prints cacheables.
	 */
	public abstract void printCacheables(PrintWriter pw);
	
	/**
	 * Checks internal consistency.
	 * Does not seem to work in a multithreaded environment
	 **/
	public abstract void validate();

	public abstract Test suite() throws Exception;
	
}