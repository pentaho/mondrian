/*
 * Created on 24.02.2004
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package mondrian.rolap.cache;

import mondrian.olap.Util;
import mondrian.rolap.RolapUtil;

import java.lang.ref.SoftReference;


/**
 * A soft reference to a cacheable.
 *
 * They are equal if and only if their underlying objects still exist and
 * are equal.
 */
public class SoftCacheableReference extends SoftReference {
	/** Unique id of the cacheable. Works beyond the grave: when
	 * cacheable is about to be garbage-collected, the referent will be
	 * null, but we'll still have the id, so we'll know what we used to
	 * point to. {@link #hashCode} and {@link #equals} use this, for the
	 * same reason. **/
	private Object cacheableId;
	public SoftCacheableReference(Cacheable referent) {
		super(referent);
		this.cacheableId = CachePool.cacheableId(referent);
	}


	/**
	 * Returns the referent {@link Cacheable}, which may be null if
	 * it has been garbage-collected.
	 */
	public Cacheable getCacheable() {
		return (Cacheable) super.get();
	}
	/**
	 * Returns the referent {@link Cacheable}. If the referent is null
	 * (because it has been garbage collected) and <code>fail</code> is
	 * true, throws an internal error.
	 *
	 * @post return != null
	 */
	public Cacheable getCacheableOrFail() {
		Cacheable cacheable = (Cacheable) super.get();
		if (cacheable != null) {
			return cacheable;
		}
		// I don't know whether this is possible. The circumstances
		// would be the following. The garbage collector decides to
		// remove this cacheable, but has not got around to calling its
		// finalize method (which calls Cacheable.removeFromCache,
		// which removes it from this list) yet. Maybe it's possible
		// in a multi-threaded environment.
		if (RolapUtil.debugOut != null) {
			RolapUtil.debugOut.println("CachePool: getCacheableOrFail failed!");
			new NullPointerException().printStackTrace(RolapUtil.debugOut);
		}
		throw Util.newInternal(
				"Cacheable's parent cache references object which " +
				"has been garbage-collected");
	}

	public int hashCode() {
		return cacheableId.hashCode();
	}

	public boolean equals(Object obj) {
		if (!(obj instanceof SoftCacheableReference)) {
			return false;
		}
		SoftCacheableReference that = (SoftCacheableReference) obj;
		return this.cacheableId.equals(that.cacheableId);
	}
	/**
	 * Returns whether this reference refers to <code>cacheable</code>.
	 * Because we've kept cacheable's unique identifier, we can do this
	 * test even when cacheable is just about to be garbage collected, and
	 * our reference has been cleared.
	 */
	public boolean refersTo(Cacheable cacheable) {
		final Object cacheableId = CachePool.cacheableId(cacheable);
		return this.cacheableId.equals(cacheableId);
	}

	public Object getCacheableId() {
		return cacheableId;
	}
}