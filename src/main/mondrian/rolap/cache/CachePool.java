package mondrian.rolap.cache;

import mondrian.rolap.RolapSchema;


/**
 * A <code>CachePool</code> manages the objects in a collection of
 * caches.
 *
 * @author av
 */
public class CachePool {

	/** Singleton. **/
	private static CachePool instance = new CachePool();

	private CachePool() {
	}
	
	public static CachePool instance() {
		return instance;
	}

	public void flush() {
		RolapSchema.clearCache();
	}

}