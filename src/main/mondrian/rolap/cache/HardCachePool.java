/*
 * Created on 24.02.2004
 *
 * To change the template for this generated file go to
 * Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
package mondrian.rolap.cache;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * A CachePool implementation keeps hard references to all objects 
 * that have been registered, even if they are not currently used. It does
 * not free any memory unless flush() is called.
 * 
 * <p>The only way to clear the caches is via the CachePool.flush() method.
 *
 * @author av
 */
public class HardCachePool extends CachePool {
	Set zombies = new HashSet();
	Set registered = new HashSet();

	public synchronized void register(Cacheable cacheable) {
		registered.add(cacheable);
	}

	public synchronized void register(Cacheable cacheable, int pinCount, Collection newlyPinned) {
		register(cacheable);
		for (int i = 0; i < pinCount; i++)
			pin(cacheable, newlyPinned);
	}

	public synchronized void pin(Cacheable cacheable, Collection newlyPinned) {
		if (!newlyPinned.add(cacheable))
			return;
		int pinCount = cacheable.getPinCount();
		cacheable.setPinCount(pinCount + 1);
	}

	public synchronized void deregister(Cacheable cacheable, boolean fromFinalizer) {
		registered.remove(cacheable);
	}

	public synchronized void flush() {
		for (Iterator it = registered.iterator(); it.hasNext();) {
			Cacheable c = (Cacheable) it.next();
			if (c.getPinCount() == 0)
				c.removeFromCache();
			else
				zombies.add(c);
		}
		registered.clear();
	}

	public synchronized void unpin(Cacheable cacheable) {
		int newPinCount = cacheable.getPinCount() - 1;
		cacheable.setPinCount(newPinCount);
		if (newPinCount == 0) {
			if (zombies.remove(cacheable))
				cacheable.removeFromCache();
		}
	}

	public synchronized void unpin(Collection pinned) {
		for (Iterator it = pinned.iterator(); it.hasNext();)
			unpin((Cacheable) it.next());
	}

	public void access(Cacheable cacheable) {
	}

	public void notify(Cacheable cacheable, double previousCost) {
	}

	public void printCacheables(PrintWriter pw) {
	}

	public void validate() {
	}

	public Test suite() throws Exception {
		return new TestSuite();
	}

}
