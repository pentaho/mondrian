/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 25 December, 2001
*/

package mondrian.rolap;
import mondrian.olap.Util;

import java.util.*;

import junit.framework.TestSuite;
import junit.framework.Test;
import junit.framework.TestCase;

/**
 * A <code>CachePool</code> manages the objects in a collection of
 * caches.
 *
 * <p>Objects in the cache are retained on the basis of their cost (size in
 * bytes), benefit (the time it would take to re-create them, or to do without
 * them), and usage.  The formula is <pre>benefit / cost * k ^ recency</pre>,
 * where <code>k</code> is a decay factor slightly less than 1, and
 * <code>recency</code> is the number of clock ticks since the object was
 * accessed.
 *
 * <p>The exponential formula for usage means that we don't have to re-sort the
 * queue each time an object is accessed. When an access occurs, the clock
 * ticks, and the global weighting factor is multiplied by k. The effective
 * recency <code>recency / weight</code> of every other object in the cache is
 * divided by <code>k</code>.
 *
 * @author jhyde
 * @since 25 December, 2001
 * @version $Id$
 **/
public class CachePool {
	private static final double decay = (1.0 - 1.0 / 32.0); // slightly < 1
	private static final double decayReciprocal = 1.0 / decay; // slightly > 1
	private double tick = 0;
	private double totalDecay = 1;
	/** Total cost of all unpinned objects. **/
	private double unpinnedCost = 0;
	private double costLimit = 0;
	/** Total cost of all objects in {@link #pinned}. **/
	private double pinnedCost;
	/** Priority queue of unpinned objects, sorted so that those with the a
	 * high cost/low benefit are at the head. **/
	private TreeSet queue;
	/** Set of objects whose pin count is greater than zero. **/
	private HashSet pinned;
	/** Set of objects which were pinned when we last flushed and are still
	 * pinned. As soon as they are unpinned, we will flush them. */
	private HashSet pinnedZombies;
	/** Sum of all objects' pin counts. For consistency checking. **/
	private int totalPinCount;
	/** Singleton. **/
	private static CachePool instance;

	CachePool(double costLimit)
	{
		this.costLimit = costLimit;
		this.queue = new TreeSet(new Comparator() {
				public int compare(Object o1, Object o2)
				{
					Cacheable c1 = (Cacheable) o1;
					Cacheable c2 = (Cacheable) o2;
					double score1 = c1.getScore();
					double score2 = c2.getScore();
					if (score1 == score2) {
						return 0;
					} else if (score1 < score2) {
						return -1;
					} else {
						return 1;
					}
				}
				public boolean equals(Object o)
				{
					return false;
				}
			});
		this.pinned = new HashSet();
		this.pinnedZombies = new HashSet();
	}

 	/** Returns or creates the singleton. **/
	public static synchronized CachePool instance()
	{
		if (instance == null) {
			instance = new CachePool(100000);
		}
		return instance;
	}

	/**
	 * Returns the number of objects in the <code>CachePool</code>.
	 **/
	int size()
	{
		return queue.size() + pinned.size();
	}

	/**
	 * Click the clock. Returns the current decay factor. Validates every so
	 * often.
	 **/
	private double tick()
	{
		tick++;
		if (true || tick % 10 == 0) {
			validate();
		}
		// Maintain the relation "totalDecay == 1 / (decay ^ tick)". Every time
		// the clock ticks, "totalDecay" increases a little, hence every
		// object's effective weight "benefit / cost * totalDecay" declines a
		// little.
		totalDecay *= decayReciprocal;
		return totalDecay;
	}

  	/**
  	 * Records that an object has just been added to a cache.
  	 **/
  	void register(Cacheable cacheable)
	{
		register(cacheable, 0, null);
	}

  	/**
  	 * Records that an object has just been added to a cache, and pins it, in
	 * an atomic operation.
  	 **/
  	public void register(Cacheable cacheable, int pinCount, Collection pinned)
	{
		cacheable.markAccessed(tick());
		queue.add(cacheable);
		double cost = cacheable.getCost();
		unpinnedCost += cost;
		while (pinCount-- > 0) {
			pin(cacheable, pinned);
		}
		flushIfNecessary();
		if (RolapUtil.debugOut != null) {
			RolapUtil.debugOut.println(
				"CachePool: add [" + cacheable +
				"], cost=" + cost +
				", size=" + size() +
				", totalCost=" + unpinnedCost);
		}
	}

	/**
	 * Removes an object from its cache.
	 **/
	void remove(Cacheable cacheable)
	{
		double cost = cacheable.getCost();
		unpinnedCost -= cost;
		Util.assertTrue(cacheable.getPinCount() == 0);
		cacheable.removeFromCache();
		boolean existed = queue.remove(cacheable);
		if (!existed) {
			throw Util.newInternal(
				"could not find [" + cacheable + "] in queue");
		}
		if (RolapUtil.debugOut != null) {
			RolapUtil.debugOut.println(
				"CachePool: remove [" + cacheable +
				"], cost=" + cost +
				", size=" + size() +
				", totalCost=" + unpinnedCost);
		}
	}

	/**
	 * Records that an object has been accessed.
	 **/
	void access(Cacheable cacheable)
	{
		cacheable.markAccessed(tick());
	}

	/**
	 * Notifies the cache that an object's score has changed, and therefore it
	 * may need to be re-positioned in the priority queue.
	 **/
	void notify(Cacheable cacheable, double previousCost)
	{
		unpinnedCost -= previousCost;
		double newCost = cacheable.getCost();
		unpinnedCost += newCost;
		boolean existed = queue.remove(cacheable);
		Util.assertTrue(existed);
		queue.add(cacheable);
	}

	/**
	 * Flushes all unpinned objects in the cache. Objects which are still
	 * pinned are marked, so they will be removed immediately that they are
	 * unpinned.
	 */
	public void flush() {
		while (!queue.isEmpty()) {
			Cacheable cacheable = (Cacheable) queue.last();
			remove(cacheable);
		}
		// Mark the still-pinned objects for death.
		pinnedZombies.addAll(pinned);
	}

	private void flushIfNecessary()
	{
		while ((unpinnedCost + pinnedCost) > costLimit && !queue.isEmpty()) {
			Cacheable cacheable = (Cacheable) queue.last();
			remove(cacheable);
		}
	}

	/**
	 * Returns the total cost of the objects in the cache.
	 **/
	double getTotalCost()
	{
		return pinnedCost + unpinnedCost;
	}

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
	 * method increments a <dfn>pin count</dfn>. {@link #unpin(Cacheable)}
	 * decrements the pin count.
	 **/
	public void pin(Cacheable cacheable, Collection newlyPinned)
	{
		int pinCount = cacheable.getPinCount();
		if (pinCount == 0) {
			pinned.add(cacheable);
			boolean existed = queue.remove(cacheable);
			if (!existed) {
				throw Util.newInternal(
					"could not find [" + cacheable + "] in queue");
			}
			double cost = cacheable.getCost();
			unpinnedCost -= cost;
			pinnedCost += cost;
		}
		cacheable.setPinCount(++pinCount);
		totalPinCount++;
		newlyPinned.add(cacheable);
		if (RolapUtil.debugOut != null) {
			RolapUtil.debugOut.println(
				"CachePool: pin [" + cacheable +
				"], pinCount=" + pinCount +
				", size=" + size() +
				", totalPinCount=" + totalPinCount);
		}
	}

	/**
	 * Unpins an object from the cache. See {@link #pin}.
	 **/
	void unpin(Cacheable cacheable)
	{
		int pinCount = cacheable.getPinCount();
		Util.assertTrue(pinCount > 0);
		if (pinCount == 1) {
			boolean existed = pinned.remove(cacheable);
			Util.assertTrue(existed);
			double cost = cacheable.getCost();
			if (pinnedZombies.remove(cacheable)) {
				// it was marked for death, so don't bother to queue it, just
				// remove it immediately
			} else {
				queue.add(cacheable);
				unpinnedCost += cost;
			}
			pinnedCost -= cost;
		}
		cacheable.setPinCount(--pinCount);
		totalPinCount--;
		if (RolapUtil.debugOut != null) {
			RolapUtil.debugOut.println(
				"CachePool: unpin [" + cacheable +
				"], pinCount=" + pinCount +
				", count=" + queue.size() +
				", totalPinCount=" + totalPinCount);
		}
		// we may just have pushed totalCost over the limit
		flushIfNecessary();
	}

	/**
	 * Unpins all elements in a list. All elements must implement {@link
	 * Cacheable}. See {@link #unpin(Cacheable)}.
	 **/
	void unpin(Collection pinned)
	{
		for (Iterator iterator = pinned.iterator(); iterator.hasNext(); ) {
			Object o = iterator.next();
			unpin((Cacheable) o);
		}
	}

	/**
	 * Checks internal consistency.
	 **/
	void validate()
	{
		Util.assertTrue(unpinnedCost <= costLimit);
		double maxScore = 0,
			totalCost = 0,
			pinnedCost = 0;
		for (Iterator elements = queue.iterator(); elements.hasNext(); ) {
			Cacheable cacheable = (Cacheable) elements.next();
			double score = cacheable.getScore(),
				cost = cacheable.getCost();
			Util.assertTrue(score > 0);
			Util.assertTrue(score >= maxScore);
			int pinCount = cacheable.getPinCount();
			Util.assertTrue(pinCount == 0);
			maxScore = score;
			totalCost += cost;
		}
		Util.assertTrue(totalCost == this.unpinnedCost);
		for (Iterator elements = pinned.iterator(); elements.hasNext(); ) {
			Cacheable cacheable = (Cacheable) elements.next();
			double cost = cacheable.getCost();
			pinnedCost += cost;
			int pinCount = cacheable.getPinCount();
			Util.assertTrue(pinCount > 0);
			totalPinCount += pinCount;
		}
		Util.assertTrue(pinnedCost == this.pinnedCost);
		// "pinnedZombies" is a subset of "pinned"
		for (Iterator zombies = pinnedZombies.iterator(); zombies.hasNext();) {
			Cacheable zombie = (Cacheable) zombies.next();
			Util.assertTrue(pinned.contains(zombie));
		}
	}

	/**
	 * Creates a JUnit testcase to test this class.
	 */
	public static Test suite() throws Exception {
		TestSuite suite = new TestSuite();
		suite.addTestSuite(CachePoolTestCase.class);
		return suite;
	}

	public static class CachePoolTestCase extends TestCase {
		public CachePoolTestCase(String s) {
			super(s);
		}
		public void test() {
			CachePool c = new CachePool(10);
			ArrayList list = new ArrayList();
			// add object of cost 3, now {3:1}
			final CacheableInt o3 = new CacheableInt(3);
			c.register(o3, 1, list);
			assertEquals(1, c.size());
			// add object of cost 6, now {3:1, 6:1}
			final CacheableInt o6 = new CacheableInt(6);
			c.register(o6, 1, list);
			assertEquals(9, (int) c.pinnedCost);
			assertEquals(9, (int) c.getTotalCost());
			// pin an already-pinned object, now {3:2, 6:1}
			c.pin(o3, list);
			assertEquals(9, (int) c.pinnedCost);
			assertEquals(9, (int) c.getTotalCost());
			assertEquals(2, c.pinned.size());
			assertEquals(2, o3.getPinCount());
			// pin a new object, taking us over the threshold, {3:2, 6:1, 8:1}
			final CacheableInt o8 = new CacheableInt(8);
			c.register(o8, 1, list);
			assertEquals(17, (int) c.getTotalCost());
			assertEquals(3, (int) c.pinned.size());
			// unpin 3, still pinned, {3:1, 6:1, 8:1}
			c.unpin(o3);
			assertEquals(17, (int) c.getTotalCost());
			assertEquals(3, (int) c.pinned.size());
			// unpin 3, it is now flushed, {6:1, 8:1}
			c.unpin(o3);
			assertEquals(14, (int) c.getTotalCost());
			assertEquals(2, (int) c.pinned.size());
			// new element, {2:3, 6:1, 8:1}
			final CacheableInt o2 = new CacheableInt(2);
			c.register(o2, 3, list);
			assertEquals(16, (int) c.getTotalCost());
			assertEquals(3, (int) c.pinned.size());
			// unpin 6, it is flushed {2:3, 8:1}
			c.unpin(o6);
			assertEquals(10, (int) c.getTotalCost());
			// unpin the others, nothing is removed, since we're at the limit
			// {2:0, 8:0}
			c.unpin(o2);
			c.unpin(o2);
			c.unpin(o2);
			c.unpin(o8);
			assertEquals(10, (int) c.getTotalCost());
			// add a 5, and the big 8 goes, {2:0, 5:0}
			final CacheableInt o5 = new CacheableInt(5);
			c.register(o5, 0, list);
			assertEquals(7, (int) c.getTotalCost());
			// pin 5, flush, the 2 goes, 5 stays, {5:1}
			c.pin(o5, list);
			c.flush();
			assertEquals(5, (int) c.getTotalCost());
			// add 3 unpinned, it stays {3:0, 5:0}
			c.register(o3, 0, list);
			assertEquals(8, (int) c.getTotalCost());
			// unpin 5, it goes because it is due to be flushed {3:0}
			c.unpin(o5);
			assertEquals(3, (int) c.getTotalCost());
		}
	}

	/**
	 * An object must implement <code>Cacheable</code> to be stored in a cache.
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

}

/**
 * Trivial {@link Cacheable} for testing purposes.
 */
class CacheableInt implements CachePool.Cacheable {
	int pinCount;
	int x;

	CacheableInt(int x) {
		this.x = x;
	}

	public void removeFromCache() {
	}

	public void markAccessed(double recency) {
	}

	public double getScore() {
		return x;
	}

	public double getCost() {
		return x;
	}

	public void setPinCount(int pinCount) {
		this.pinCount = pinCount;
	}

	public int getPinCount() {
		return pinCount;
	}
}

// End CachePool.java
