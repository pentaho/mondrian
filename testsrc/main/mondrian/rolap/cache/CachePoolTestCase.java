/*
 * Copyright 2003 by Alphablox Corp. All rights reserved.
 *
 * Created by gjohnson
 * Last change: $Modtime: $
 * Last author: $Author$
 * Revision: $Revision$
 */

package mondrian.rolap.cache;

import java.util.ArrayList;

import junit.framework.TestCase;

public class CachePoolTestCase extends TestCase {
    public CachePoolTestCase(String s) {
        super(s);
    }
    
    public void testCache() {
        MondrianCachePool c = new MondrianCachePool(10);
        ArrayList list = new ArrayList();
        // add object of cost 3, now {3:1}
        final CacheableInt o3 = new CacheableInt(3);
        c.register(o3, 1, list);
        assertEquals(1, c.size());
        // add object of cost 6, now {3:1, 6:1}
        final CacheableInt o6 = new CacheableInt(6);
        c.register(o6, 1, list);
        assertEquals(9, (int) c.getPinnedCost());
        assertEquals(9, (int) c.getTotalCost());
        // pin an already-pinned object, now {3:2, 6:1}
        c.pin(o3, list);
        assertEquals(9, (int) c.getPinnedCost());
        assertEquals(9, (int) c.getTotalCost());
        assertEquals(2, c.getPinned().size());
        assertEquals(2, o3.getPinCount());
        // pin a new object, taking us over the threshold, {3:2, 6:1, 8:1}
        final CacheableInt o8 = new CacheableInt(8);
        c.register(o8, 1, list);
        assertEquals(17, (int) c.getTotalCost());
        assertEquals(3, c.getPinned().size());
        // unpin 3, still getPinned(), {3:1, 6:1, 8:1}
        c.unpin(o3);
        assertEquals(17, (int) c.getTotalCost());
        assertEquals(3, c.getPinned().size());
        // unpin 3, it is now flushed, {6:1, 8:1}
        c.unpin(o3);
        assertEquals(14, (int) c.getTotalCost());
        assertEquals(2, c.getPinned().size());
        // new element, {2:3, 6:1, 8:1}
        final CacheableInt o2 = new CacheableInt(2);
        c.register(o2, 3, list);
        assertEquals(16, (int) c.getTotalCost());
        assertEquals(3, c.getPinned().size());
        // unpin 6, it is flushed {2:3, 8:1}
        c.unpin(o6);
        assertEquals(10, (int) c.getTotalCost());
        // unpin the others, nothing is removed, since we're at the limit
        // {2:0, 8:0}
        c.unpin(o2);
        c.unpin(o2);
        // pin an unregistered object -- should be registered automatically
        final CacheableInt o20 = new CacheableInt(20);
        c.pin(o20, list);
        assertTrue(list.contains(o20));
        assertEquals(1, o20.getPinCount());
        c.unpin(o20);
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
        // add 3 ungetPinned(), it stays {3:0, 5:0}
        c.register(o3, 0, list);
        assertEquals(8, (int) c.getTotalCost());
        // unpin 5, it goes because it is due to be flushed {3:0}
        c.unpin(o5);
        assertEquals(3, (int) c.getTotalCost());
    }

    /**
     * Trivial {@link Cacheable} for testing purposes.
     */
    static class CacheableInt implements Cacheable {
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

}
