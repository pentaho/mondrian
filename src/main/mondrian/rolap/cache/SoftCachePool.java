package mondrian.rolap.cache;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import mondrian.olap.Util;

/**
 * A CachePool implementation that allows the garbage collector to
 * reclaim all objects that are not currently used. It uses all available
 * memory for caching.
 *
 * <p>Must specify minimum memory size to the JVM, e.g. -Xms128m, otherwise
 * all objects will be garbage collected immediately.
 *
 * <p>It forces removal from a cache only if flush() is called. Otherwise
 * Cacheables will be removed by the garbage collector.
 *
 * @author av
 */
public class SoftCachePool extends CachePool {

	/**
	 * contains soft references to all registered Cacheables.
	 * key is cacheableId(), value is a SoftCacheableReference
	 */
	Map registered = new HashMap();

	/**
	 * contains hard references to Cacheables that must not be garbage collected.
	 * <p>invariant: All entries are Cacheables with pinCount() > 0.
	 * <p>invariant: pinned is a subset of registered.
	 */
	Set pinned = new HashSet();

	/**
	 * contains hard references to Cacheables that are to be
	 * removed as soon as their pinCount reaches zero.
	 * <p>invariant: zombies is a subset of pinned.
	 */
	Set zombies = new HashSet();

	public synchronized void register(Cacheable cacheable) {
		SoftCacheableReference ref = new SoftCacheableReference(cacheable);
		registered.put(ref.getCacheableId(), ref);
	}

	public synchronized void deregister(Cacheable cacheable, boolean fromFinalizer) {
		// dont throw exceptions here because this will prevent garbage
		// collection when called from finalize()
		registered.remove(cacheableId(cacheable));
	}

	public synchronized void register(Cacheable cacheable, int pinCount, Collection newlyPinned) {
		register(cacheable);
		for (int i = 0; i < pinCount; i++)
			pin(cacheable, newlyPinned);
	}

	public synchronized void unpin(Collection pinned) {
		for (Iterator it = pinned.iterator(); it.hasNext();)
			unpin((Cacheable) it.next());
	}

	public synchronized void flush() {
		for (Iterator it = registered.values().iterator(); it.hasNext();) {
			SoftCacheableReference ref = (SoftCacheableReference) it.next();
			Cacheable c = (Cacheable) ref.getCacheable();
			if (c == null) {
				it.remove();
			} else if (c.getPinCount() == 0) {
				it.remove();
				c.removeFromCache();
			} else {
				zombies.add(c);
			}
		}
		//validate();
	}

	/**
	 * prevents garbabe collector from discarding <code>cacheable</code>.
	 */
	public synchronized void pin(Cacheable cacheable, Collection newlyPinned) {
		if (!newlyPinned.add(cacheable))
			return;
		int pinCount = cacheable.getPinCount();
		// avoid HashSet access unless necessary
		if (pinCount == 0)
			pinned.add(cacheable);
		cacheable.setPinCount(pinCount + 1);
		//validate();
	}

	/**
	 * if no more clients refer to cacheable, remove it
	 * from the list of pinned cacheables.
	 */
	public synchronized void unpin(Cacheable cacheable) {
		int newPinCount = cacheable.getPinCount() - 1;
		Util.assertTrue(newPinCount >= 0);
		cacheable.setPinCount(newPinCount);
		if (newPinCount == 0) {
			Util.assertTrue(pinned.remove(cacheable));
			// if its a zombie, force a remove
			// otherwise let the garbage collector remove it
			if (zombies.remove(cacheable))
				cacheable.removeFromCache();
		}
		//validate();
	}

	public synchronized void printCacheables(PrintWriter pw) {
		pw.println("registered: ");
		for (Iterator it = registered.keySet().iterator(); it.hasNext();) {
			Cacheable c = (Cacheable) it.next();
			pw.println(c);
		}
		pw.println("pinned: ");
		for (Iterator it = pinned.iterator(); it.hasNext();) {
			Cacheable c = (Cacheable) it.next();
			pw.println(c);
		}
		pw.println("zombies: ");
		for (Iterator it = zombies.iterator(); it.hasNext();) {
			Cacheable c = (Cacheable) it.next();
			pw.println(c);
		}
	}

	public synchronized void validate() {
		// pinned has pinCount > 0
		for (Iterator it = pinned.iterator(); it.hasNext();) {
			Cacheable c = (Cacheable) it.next();
			Util.assertTrue(c.getPinCount() > 0);
			Util.assertTrue(registered.containsKey(cacheableId(c)));
		}

		// zombies is a subset of pinned
		for (Iterator it = zombies.iterator(); it.hasNext();)
			Util.assertTrue(pinned.contains(it.next()));
	}

	/** does nothing */
	public void access(Cacheable cacheable) {
	}

	/** does nothing */
	public void notify(Cacheable cacheable, double previousCost) {
	}
}
