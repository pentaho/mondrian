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
import java.util.Iterator;
import java.util.TreeSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Collection;

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
	private double totalCost = 0;
	private double costLimit = 0;
	/** Total cost of all objects in {@link #pinned}. **/
	private double pinnedCost;
	/** Total number of pins for all objects in cache. **/
	private TreeSet queue;
	/** Set of objects whose pin count is greater than zero. **/
	private HashSet pinned;
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
		totalCost += cost;
		while (pinCount-- > 0) {
			pin(cacheable, pinned);
		}
		flushIfNecessary();
		if (RolapUtil.debugOut != null) {
			RolapUtil.debugOut.println(
				"CachePool: add [" + cacheable +
				"], cost=" + cost +
				", size=" + size() +
				", totalCost=" + totalCost);
		}
	}

	/**
	 * Removes an object from its cache.
	 **/
	void remove(Cacheable cacheable)
	{
		double cost = cacheable.getCost();
		totalCost -= cost;
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
				", totalCost=" + totalCost);
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
		totalCost -= previousCost;
		double newCost = cacheable.getCost();
		totalCost += newCost;
		boolean existed = queue.remove(cacheable);
		Util.assertTrue(existed);
		queue.add(cacheable);
	}

	private void flushIfNecessary()
	{
		while (totalCost > costLimit) {
			Cacheable cacheable = (Cacheable) queue.last();
			remove(cacheable);
		}
	}

	/**
	 * Returns the total cost of the objects in the cache.
	 **/
	double getTotalCost()
	{
		return totalCost;
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
			totalCost -= cost;
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
			queue.add(cacheable);
			double cost = cacheable.getCost();
			totalCost += cost;
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
		Util.assertTrue(totalCost <= costLimit);
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
		Util.assertTrue(totalCost == this.totalCost);
		for (Iterator elements = pinned.iterator(); elements.hasNext(); ) {
			Cacheable cacheable = (Cacheable) elements.next();
			double cost = cacheable.getCost();
			pinnedCost += cost;
			int pinCount = cacheable.getPinCount();
			Util.assertTrue(pinCount > 0);
			totalPinCount += pinCount;
		}
		Util.assertTrue(pinnedCost == this.pinnedCost);
	}

	public interface Cacheable
	{
//  		/** Retrieves the cache that this object belongs to. **/
//  		Cache getCache();
//  		/** You must call this method when the object is first put in the
//  		 * cache. **/
//  		void registerInCachePool();
//  		/** Retrieves the key used to place this object in the cache. **/
//  		Object getKey();
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
	};

//  	interface Queue
//  	{
//  		Object last();
//  		Iterator elements();
//  		Object remove(Object o);
//  		void add(Object o);
//  	};
}

// End CachePool.java
