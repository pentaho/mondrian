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
import mondrian.olap.MondrianProperties;

import java.util.*;
import java.io.PrintWriter;
import java.lang.ref.SoftReference;

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
 * <p>The cache pool is not infallible, so to allow the garbage collector to do
 * its job, it uses {@link SoftReference soft references}. (Except on pinned
 * objects.)
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
	 * high cost/low benefit are at the head. Soft references are used.
	 * @synchronization Lock the cache pool, not the queue.
	 **/
	private Queue queue;
	/** Set of objects whose pin count is greater than zero. Hard references
	 * are used, to keep the garbage collector at bay. **/
	private HashSet pinned;
	/** Set of objects which were pinned when we last flushed and are still
	 * pinned. As soon as they are unpinned, we will flush them. Hard
	 * references. */
	private HashSet pinnedZombies;
	/** Sum of all objects' pin counts. For consistency checking. **/
	private int totalPinCount;
	/** Singleton. **/
	private static CachePool instance;
	/** List of all strings representing all {@link Cacheable} objects which
	 * have not yet been finalized. Hard references to strings. */
	private HashMap mapCacheableIdToCost = new HashMap();

	CachePool(double costLimit)
	{
		this.costLimit = costLimit;
		this.queue = new Queue(new MyComparator());
		this.pinned = new HashSet();
		this.pinnedZombies = new HashSet();
	}

	/**
	 * Queue is a priority-queue. It is implemented in terms of a sorted
	 * {@link ArrayList}; a heap-based implementation would be better.
	 */
	private static class Queue {
		private Comparator comparator;
		private ArrayList list = new ArrayList();
		Queue(Comparator comparator) {
			this.comparator = comparator;
		}
		int size() {
			return list.size();
		}
		void add(Object o) {
			for (int i = 0; i < list.size(); i++) {
				Object o2 = list.get(i);
				if (comparator.compare(o, o2) < 0) {
					list.add(i, o);
					return;
				}
			}
			list.add(o);
		}
		boolean isEmpty() {
			return list.isEmpty();
		}
		Object removeLast() {
			return list.remove(list.size() - 1);
		}
		boolean remove(Object o) {
			return list.remove(o);
		}
		Iterator iterator() {
			return list.iterator();
		}
	}

	private static class MyComparator implements Comparator {
		public int compare(Object o1, Object o2)
		{
			SoftCacheableReference ref1 = (SoftCacheableReference) o1,
					ref2 = (SoftCacheableReference) o2;
			Cacheable c1 = ref1.getCacheable(),
					c2 = ref2.getCacheable();
			double score1 = c1 == null ? 0 : c1.getScore(),
					score2 = c2 == null ? 0 : c2.getScore();
			if (score1 == score2) {
				return 0;
			} else if (score1 < score2) {
				return -1;
			} else {
				return 1;
			}
		}
		public boolean equals(Object o) {
			return false;
		}
	}

 	/** Returns or creates the singleton. **/
	public static synchronized CachePool instance()
	{
		if (instance == null) {
			final int costLimit =
					MondrianProperties.instance().getCachePoolCostLimit();
			instance = new CachePool(costLimit);
		}
		return instance;
	}

	/**
	 * Returns the number of objects in the <code>CachePool</code>.
	 **/
	synchronized int size() {
		return queue.size() + pinned.size();
	}

	/**
	 * Click the clock. Returns the current decay factor. Validates every so
	 * often.
	 **/
	private double tick()
	{
		tick++;
		if (tick % 1000 == 0) {
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
  	public synchronized void register(
			Cacheable cacheable, int pinCount, Collection pinned) {
		String id = cacheableId(cacheable);
		double cost = cacheable.getCost();
		if (mapCacheableIdToCost.put(id, new Double(cost)) != null) {
			throw Util.newInternal("Cacheable '" + cacheable +
					"' added to CachePool more than once");
		}
		cacheable.markAccessed(tick());
		// Yes, add to queue even if pinCount > 0, because pin() assumes that
		// the object is on the queue.
		queue.add(new SoftCacheableReference(cacheable));
		unpinnedCost += cost;
//		System.out.println("unpinnedCost a now " + unpinnedCost + ", pinnedCost=" + pinnedCost);
		if (RolapUtil.debugOut != null) {
			RolapUtil.debugOut.println(
				"CachePool: register [" + cacheable +
				"], cost=" + cost +
				", size=" + size() +
				", totalCost=" + unpinnedCost);
		}
		while (pinCount-- > 0) {
			pin(cacheable, pinned);
		}
		flushIfNecessary();
	}

	/**
	 * Returns the last object on the queue and returns it.
	 *
	 * <p><b>Note to developers</b>: This method must remain private and be
	 * called from a synchronized context.
	 */
	private Cacheable removeLast() {
		while (!queue.isEmpty()) {
			SoftCacheableReference ref = (SoftCacheableReference) queue.removeLast();
			Cacheable cacheable = ref.getCacheable();
			if (cacheable != null) {
				deregisterInternal(cacheable);
				return cacheable;
			}
		}
		// There were no non-finalized objects on the queue.
		return null;
	}

	/**
	 * A {@link Cacheable} <em>must</em> call this from its
	 * <code>finalize</code> method.
	 */
	public void deregister(Cacheable cacheable, boolean fromFinalizer) {
		synchronized (this) {
			deregisterInternal(cacheable);
		}
		// Remove it from the queue. (If this method is called from the
		// cacheable's finalize method, the soft reference to it will
		// already have been nullifed.)
		boolean existed = queue.remove(new SoftCacheableReference(cacheable));
		Util.discard(existed);
	}

	/**
	 * As {@link #deregister}, but avoids synchronization overhead.
	 *
	 * <p><b>Note to developers</b>: This method must remain private and be
	 * called from a synchronized context.
	 */
	private void deregisterInternal(Cacheable cacheable) {
		String id = cacheableId(cacheable);
		Double registeredCost = (Double) this.mapCacheableIdToCost.remove(id);
		if (registeredCost == null) {
			// We've already deregistered it.
			return;
		}
		double cost = cacheable.getCost();
		if (cost != registeredCost.doubleValue()) {
			throw Util.newInternal(
					id + " had cost " + cost +
					" when registered, now has cost " + registeredCost);
		}
		unpinnedCost -= cost;
//		System.out.println("unpinnedCost b now " + unpinnedCost + ", pinnedCost=" + pinnedCost);
		Util.assertTrue(cacheable.getPinCount() == 0);
		cacheable.removeFromCache();
		if (RolapUtil.debugOut != null) {
			RolapUtil.debugOut.println(
					"CachePool: deregister [" + cacheable +
					"], registeredCost=" + cost +
					", size=" + size() +
					", totalCost=" + unpinnedCost);
		}
	}

	/**
	 * Records that an object has been accessed.
	 **/
	void access(Cacheable cacheable)
	{
		checkRegistered(cacheable);
		cacheable.markAccessed(tick());
	}

	/**
	 * Notifies the cache that an object's score has changed, and therefore it
	 * may need to be re-positioned in the priority queue.
	 **/
	public synchronized void notify(Cacheable cacheable, double previousCost)
	{
		String id = cacheableId(cacheable);
		double newCost = cacheable.getCost();
		double deltaCost = newCost - previousCost;
		int pinCount = cacheable.getPinCount();
		switch (pinCount) {
		case 0:
			unpinnedCost += deltaCost;
			SoftCacheableReference ref = new SoftCacheableReference(cacheable);
			boolean existed = queue.remove(ref);
			Util.assertTrue(existed);
			queue.add(ref);
			break;
		case 1:
			pinnedCost += deltaCost;
			break;
		default:
		}
		if (RolapUtil.debugOut != null) {
			RolapUtil.debugOut.println(
				"CachePool: notify [" + cacheable +
					"], previousCost=" + previousCost +
				", newCost=" + newCost +
				", pinCount=" + pinCount);
		}
//		System.out.println("unpinnedCost d now " + unpinnedCost + ", pinnedCost=" + pinnedCost);
		Double d = (Double) mapCacheableIdToCost.put(id, new Double(newCost));
//		System.out.println("oldcost=" + d + ",  ?=" + (d.doubleValue() != previousCost));
		if (d == null) {
			throw Util.newInternal("not registered");
		} else if (d.doubleValue() != previousCost) {
			System.exit(1);
			throw Util.newInternal("wrong cost");
		}
	}

	/**
	 * Flushes all unpinned objects in the cache. Objects which are still
	 * pinned are marked, so they will be removed immediately that they are
	 * unpinned.
	 */
	public synchronized void flush() {
		while (removeLast() != null) {
		}
		// Mark the still-pinned objects for death.
		pinnedZombies.addAll(pinned);
	}

	/**
	 * <b>Note to developers</b>: Must be called from a synchronized context.
	 */
	private void flushIfNecessary()
	{
		while ((unpinnedCost + pinnedCost) > costLimit && !queue.isEmpty()) {
			removeLast();
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
	 * method increments a <dfn>pin count</dfn>. {@link
	 * #unpin(Cache.Cacheable)} decrements the pin count.
	 *
	 * <p>The object is not pinned if it is already in the
	 * <code>newlyPinned</code> list.
	 **/
	public synchronized void pin(Cacheable cacheable, Collection newlyPinned) {
		if (!newlyPinned.add(cacheable)) {
			return;
		}
		double cost = checkRegistered(cacheable);
//		printCacheables(RolapUtil.debugOut);
		int pinCount = cacheable.getPinCount();
		Util.assertTrue(pinCount >= 0);
		if (pinCount == 0) {
			pinned.add(cacheable);
			SoftCacheableReference ref = new SoftCacheableReference(cacheable);
			boolean existed = queue.remove(ref);
			if (!existed) {
				throw Util.newInternal(
					"could not find [" + cacheable + "] in queue");
			}
			unpinnedCost -= cost;
			pinnedCost += cost;
//			System.out.println("unpinnedCost e now " + unpinnedCost + ", pinnedCost=" + pinnedCost);
		}
		cacheable.setPinCount(++pinCount);
		totalPinCount++;
		if (RolapUtil.debugOut != null) {
			RolapUtil.debugOut.println(
				"CachePool: pin [" + cacheable +
				"], pinCount=" + pinCount +
				", size=" + size() +
				", totalPinCount=" + totalPinCount);
		}
	}

	/**
	 * Makes sure that <code>cacheable</code> is registered in this cache pool,
	 * throws an error if it is not registered or if its cost is not the same
	 * as the one provided to {@link #register} or {@link #notify}. Returns the
	 * cost.
	 *
	 * @synchronization Must be called from synchronized context.
	 */
	private double checkRegistered(Cacheable cacheable) {
		String id = cacheableId(cacheable);
		Double registeredCost = (Double) mapCacheableIdToCost.get(id);
		if (registeredCost == null) {
			throw Util.newInternal(id + " is not registered");
		}
		double cost = cacheable.getCost();
		if (registeredCost.doubleValue() != cost) {
			throw Util.newInternal(id + "'s cost has changed");
		}
		return cost;
	}

	/**
	 * Returns whether <code>cacheable</code> is registered.
	 */
	synchronized boolean isRegistered(Cacheable cacheable) {
		String id = cacheableId(cacheable);
		return mapCacheableIdToCost.get(id) != null;
	}

	/**
	 * Unpins an object from the cache. See {@link #pin}.
	 **/
	synchronized void unpin(Cacheable cacheable)
	{
		double cost = checkRegistered(cacheable);
//		printCacheables(RolapUtil.debugOut);
		int pinCount = cacheable.getPinCount() - 1;
		cacheable.setPinCount(pinCount);
		Util.assertTrue(pinCount >= 0);
		if (pinCount == 0) {
			boolean existed = pinned.remove(cacheable);
			Util.assertTrue(existed);
			if (pinnedZombies.remove(cacheable)) {
				// it was marked for death, so don't bother to queue it, just
				// remove it immediately
			} else {
				queue.add(new SoftCacheableReference(cacheable));
				unpinnedCost += cost;
			}
			pinnedCost -= cost;
//			System.out.println("unpinnedCost f now " + unpinnedCost + ", pinnedCost=" + pinnedCost);
		}
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

	private static String cacheableId(Cacheable cacheable) {
		return cacheable.getClass().getName() + "@" +
				Integer.toHexString(System.identityHashCode(cacheable));
	}

	/**
	 * Prints cacheables.
	 */
	public void printCacheables(PrintWriter pw) {
		pw.println("Cacheable objects which have not been finalized: count=" +
				mapCacheableIdToCost.size() + ": {");
		for (Iterator iterator = mapCacheableIdToCost.keySet().iterator(); iterator.hasNext();) {
			String s = (String) iterator.next();
			Double cost = (Double) mapCacheableIdToCost.get(s);
			pw.println(s + ", cost=" + cost);
		}
		pw.println("}");
		pw.flush();
	}

	/**
	 * Checks internal consistency.
	 **/
	synchronized void validate()
	{
//		printCacheables(RolapUtil.debugOut);
		// "pinnedZombies" is a subset of "pinned"
		for (Iterator zombies = pinnedZombies.iterator(); zombies.hasNext();) {
			Cacheable zombie = (Cacheable) zombies.next();
			if (!pinned.contains(zombie)) {
				throw newValidateFailed("element in pinned is not in pinnedZombies");
			}
		}
		if (unpinnedCost > costLimit) {
			throw newValidateFailed("unpinnedCost{" + unpinnedCost + "} > costLimit {" + costLimit + "}");
		}
		double maxScore = 0,
			unpinnedCost = 0,
			pinnedCost = 0;
		int totalPinCount = 0;
		// ghosts are objects we can't reach because they are just about to be
		// garbage collected
		int ghosts = 0;
		for (Iterator elements = queue.iterator(); elements.hasNext(); ) {
			SoftCacheableReference ref = (SoftCacheableReference) elements.next();
			Cacheable cacheable = ref.getCacheable();
			if (cacheable == null) {
				ghosts++;
				continue;
			}
			double cost = checkRegistered(cacheable),
					score = cacheable.getScore();
			if (score <= 0) {
				throw newValidateFailed("score{" + score + "} < 0");
			}
			if (score < maxScore) {
				throw newValidateFailed("score{" + score + "} < maxScore{" + maxScore + "}");
			}
			int pinCount = cacheable.getPinCount();
			if (pinCount != 0) {
				throw newValidateFailed("pinCount{" + pinCount + "} != 0");
			}
			maxScore = score;
			unpinnedCost += cost;
		}
		if (unpinnedCost != this.unpinnedCost && ghosts == 0) {
			throw newValidateFailed("unpinnedCost{" + unpinnedCost + "} != this.unpinnedCost {" + this.unpinnedCost + "}");
		}
		for (Iterator elements = pinned.iterator(); elements.hasNext(); ) {
			Cacheable cacheable = (Cacheable) elements.next();
			double cost = checkRegistered(cacheable);
			pinnedCost += cost;
			int pinCount = cacheable.getPinCount();
			if (pinCount <= 0) {
				throw newValidateFailed("pinCount{" + pinCount + "} <= 0");
			}
			totalPinCount += pinCount;
		}
		if (pinnedCost != this.pinnedCost) {
			throw newValidateFailed("pinnedCost{" + pinnedCost + "} != this.pinnedCost{" + this.pinnedCost + "}");
		}
		if (totalPinCount != this.totalPinCount) {
			throw newValidateFailed("totalPinCount{" + totalPinCount + "} != this.totalPinCount{" + this.totalPinCount + "}");
		}
	}

	private RuntimeException newValidateFailed(String s) {
		if (RolapUtil.debugOut != null) {
			RolapUtil.debugOut.println("CachePool.validate failed: " + s);
		}
		return Util.newInternal(s);
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
			assertEquals(3, c.pinned.size());
			// unpin 3, still pinned, {3:1, 6:1, 8:1}
			c.unpin(o3);
			assertEquals(17, (int) c.getTotalCost());
			assertEquals(3, c.pinned.size());
			// unpin 3, it is now flushed, {6:1, 8:1}
			c.unpin(o3);
			assertEquals(14, (int) c.getTotalCost());
			assertEquals(2, c.pinned.size());
			// new element, {2:3, 6:1, 8:1}
			final CacheableInt o2 = new CacheableInt(2);
			c.register(o2, 3, list);
			assertEquals(16, (int) c.getTotalCost());
			assertEquals(3, c.pinned.size());
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

	/**
	 * A soft reference to a cacheable.
	 *
	 * They are equal if and only if their underlying objects still exist and
	 * are equal.
	 */
	public static class SoftCacheableReference extends SoftReference {
		/** Unique id of the cacheable. Works beyond the grave: when
		 * cacheable is about to be garbage-collected, the referent will be
		 * null, but we'll still have the id, so we'll know what we used to
		 * point to. {@link #hashCode} and {@link #equals} use this, for the
		 * same reason. **/
		private String cacheableId;
		public SoftCacheableReference(Cacheable referent) {
			super(referent);
			this.cacheableId = cacheableId(referent);
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
			final String cacheableId = cacheableId(cacheable);
			return this.cacheableId.equals(cacheableId);
		}
	}
}

/**
 * Trivial {@link CachePool.Cacheable} for testing purposes.
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
