/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
// Copyright (C) 2011-2011 Julian Hyde and others
// All Rights Reserved.
*/
package mondrian.olap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import mondrian.util.Pair;

/**
 * Provides hooks for recording timing information of components of Query
 * execution.
 *
 * <p>NOTE: This class is experimental and subject to change/removal
 * without notice.
 *
 * <p>Code that executes as part of a Query can call
 * {@link QueryTiming#markStart(String)}
 * before executing, and {@link QueryTiming#markEnd(String)} afterwards, or can
 * track execution times manually and call
 * {@link QueryTiming#markFull(String, long)}.
 *
 * <p>After Query execution, a QueryTiming object can be retrieved from
 * {@link Query#getQueryTiming()}.
 *
 * @author jbarnett
 * @version $Id$
 */
public class QueryTiming {
    private static ThreadLocal<QueryTiming> tlocal =
        new InheritableThreadLocal<QueryTiming>() {
            public QueryTiming initialValue() {
                return null;
            }
        };

    protected static void init() {
        tlocal.set(new QueryTiming());
    }

    protected static QueryTiming done() {
        QueryTiming ret = tlocal.get();
        tlocal.remove();
        return ret;
    }

    /**
     * Mark the start of a Query component's execution
     * @param name Name of the component
     */
    public static void markStart(String name) {
        QueryTiming queryTiming = tlocal.get();
        if (queryTiming != null) {
            queryTiming.markStartInternal(name);
        }
    }

    /**
     * Mark the end of a Query component's execution
     * @param name Name of the component
     */
    public static void markEnd(String name) {
        long tstamp = System.currentTimeMillis();
        QueryTiming queryTiming = tlocal.get();
        if (queryTiming != null) {
            queryTiming.markEndInternal(name, tstamp);
        }
    }

    /**
     * Mark the duration of a Query component's execution
     * @param name Name of the component
     * @param duration Duration of the execution
     */
    public static void markFull(String name, long duration) {
        QueryTiming queryTiming = tlocal.get();
        if (queryTiming != null) {
            queryTiming.markFullInternal(name, duration);
        }
    }

    private static class TimingInfo {
        private final String name;
        private final long startTime;
        private long endTime;

        private TimingInfo(String name) {
            this.name = name;
            this.startTime = System.currentTimeMillis();
        }

        private void markEnd(long tstamp) {
            this.endTime = tstamp;
        }
    }

    private Stack<TimingInfo> currentTimings;
    private Map<String, List<Pair<Long, Long>>> timings;
    private Map<String, Pair<Long, Long>> fullTimings;

    private void markStartInternal(String name) {
        if (currentTimings == null) {
            currentTimings = new Stack<TimingInfo>();
        }
        currentTimings.push(new TimingInfo(name));
    }

    private void markEndInternal(String name, long tstamp) {
        if (currentTimings == null
            || currentTimings.isEmpty()
            || !currentTimings.peek().name.equals(name))
        {
            throw new IllegalStateException("end but no start for " + name);
        }
        TimingInfo finished = currentTimings.pop();
        assert finished.name.equals(name);
        finished.markEnd(tstamp);

        if (timings == null) {
            timings = new HashMap<String, List<Pair<Long, Long>>>();
        }
        if (!timings.containsKey(finished.name)) {
            timings.put(finished.name, new ArrayList<Pair<Long, Long>>());
        }
        List<Pair<Long, Long>> timingList = timings.get(finished.name);
        timingList.add(Pair.of(finished.startTime, finished.endTime));
    }

    private void markFullInternal(String name, long duration) {
        if (fullTimings == null) {
            fullTimings = new HashMap<String, Pair<Long, Long>>();
        }
        if (!fullTimings.containsKey(name)) {
            fullTimings.put(name, new Pair<Long, Long>(0l, 0l));
        }
        Pair<Long, Long> p = fullTimings.get(name);
        p.left = p.left + 1;
        p.right = p.right + duration;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (timings != null) {
            for (Map.Entry<String, List<Pair<Long, Long>>> entry
                : timings.entrySet())
            {
                if (sb.length() > 0) {
                    sb.append("\t");
                }
                long total = 0;
                for (Pair<Long, Long> times : entry.getValue()) {
                    total += (times.right - times.left);
                }
                int count = entry.getValue().size();
                sb.append(entry.getKey())
                    .append(" invoked ")
                    .append(count)
                    .append(" times for total of ")
                    .append(total)
                    .append("ms.  (Avg. ")
                    .append(total / count)
                    .append("ms/invocation)");
            }
        }
        if (fullTimings != null) {
            for (Map.Entry<String, Pair<Long, Long>> entry
                : fullTimings.entrySet())
            {
                if (sb.length() > 0) {
                    sb.append("\t");
                }
                sb.append(entry.getKey())
                    .append(" invoked ")
                    .append(entry.getValue().left)
                    .append(" times for total of ")
                    .append(entry.getValue().right)
                    .append("ms.  (Avg. ")
                    .append(entry.getValue().right / entry.getValue().left)
                    .append("ms/invocation)");
            }
        }
        return sb.toString();
    }

    /**
     * @return a collection of all Query component names
     */
    public Collection<String> getTimingKeys() {
        Set<String> keys = new HashSet<String>();
        if (timings != null) {
            keys.addAll(timings.keySet());
        }
        if (fullTimings != null) {
            keys.addAll(fullTimings.keySet());
        }
        return keys;
    }

    /**
     * @param key Name of the Query component to get timing information on
     * @return a List of durations
     */
    public List<Long> getTimings(String key) {
        List<Long> timingList = new ArrayList<Long>();
        if (timings != null) {
            List<Pair<Long, Long>> regTime = timings.get(key);
            if (regTime != null) {
                for (Pair<Long, Long> timing : regTime) {
                    timingList.add(timing.right - timing.left);
                }
            }
        }
        if (fullTimings != null) {
            Pair<Long, Long> qTime = fullTimings.get(key);
            if (qTime != null) {
                for (int i = 0; i < qTime.left; i++) {
                    timingList.add(qTime.right);
                }
            }
        }
        return timingList;
    }
}

// End QueryTiming.java
