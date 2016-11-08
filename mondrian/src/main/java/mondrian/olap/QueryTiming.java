/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap;

import mondrian.util.ArrayStack;

import java.util.*;

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
 * <p>To read timing information, add a handler to the statement using
 * {@link mondrian.server.Statement#enableProfiling} and implement the
 * {@link mondrian.spi.ProfileHandler#explain(String, QueryTiming)} method.
 *
 * @author jbarnett
 */
public class QueryTiming {
    private boolean enabled;
    private final ArrayStack<TimingInfo> currentTimings =
        new ArrayStack<TimingInfo>();
    private final Map<String, List<StartEnd>> timings =
        new HashMap<String, List<StartEnd>>();
    private final Map<String, DurationCount> fullTimings =
        new HashMap<String, DurationCount>();

    /**
     * Initializes (or re-initializes) a query timing, also setting whether
     * enabled. All previous stats are removed.
     *
     * @param enabled Whether to collect stats in future
     */
    public synchronized void init(boolean enabled) {
        this.enabled = enabled;
        currentTimings.clear();
        timings.clear();
        fullTimings.clear();
    }

    public void done() {
    }

    /**
     * Marks the start of a Query component's execution.
     *
     * @param name Name of the component
     */
    public synchronized final void markStart(String name) {
        if (enabled) {
            markStartInternal(name);
        }
    }

    /**
     * Marks the end of a Query component's execution.
     *
     * @param name Name of the component
     */
    public synchronized final void markEnd(String name) {
        if (enabled) {
            long tstamp = System.currentTimeMillis();
            markEndInternal(name, tstamp);
        }
    }

    /**
     * Marks the duration of a Query component's execution.
     *
     * @param name Name of the component
     * @param duration Duration of the execution
     */
    public synchronized final void markFull(String name, long duration) {
        if (enabled) {
            markFullInternal(name, duration);
        }
    }

    private void markStartInternal(String name) {
        currentTimings.push(new TimingInfo(name));
    }

    private void markEndInternal(String name, long tstamp) {
        if (currentTimings.isEmpty()
            || !currentTimings.peek().name.equals(name))
        {
            throw new IllegalStateException("end but no start for " + name);
        }
        TimingInfo finished = currentTimings.pop();
        assert finished.name.equals(name);
        finished.markEnd(tstamp);

        List<StartEnd> timingList = timings.get(finished.name);
        if (timingList == null) {
            timingList = new ArrayList<StartEnd>();
            timings.put(finished.name, timingList);
        }
        timingList.add(new StartEnd(finished.startTime, finished.endTime));
    }

    private void markFullInternal(String name, long duration) {
        DurationCount p = fullTimings.get(name);
        if (p == null) {
            p = new DurationCount();
            fullTimings.put(name, p);
        }
        p.count++;
        p.duration += duration;
    }

    public synchronized String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, List<StartEnd>> entry
            : timings.entrySet())
        {
            if (sb.length() > 0) {
                sb.append(Util.nl);
            }
            long total = 0;
            for (StartEnd durection : entry.getValue()) {
                total += (durection.endTime - durection.startTime);
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
        for (Map.Entry<String, DurationCount> entry
            : fullTimings.entrySet())
        {
            if (sb.length() > 0) {
                sb.append(Util.nl);
            }
            sb.append(entry.getKey())
                .append(" invoked ")
                .append(entry.getValue().count)
                .append(" times for total of ")
                .append(entry.getValue().duration)
                .append("ms.  (Avg. ")
                .append(entry.getValue().duration / entry.getValue().count)
                .append("ms/invocation)");
        }
        return sb.toString();
    }

    /**
     * @return a collection of all Query component names
     */
    public synchronized Collection<String> getTimingKeys() {
        Set<String> keys = new HashSet<String>();
        keys.addAll(timings.keySet());
        keys.addAll(fullTimings.keySet());
        return keys;
    }

    /**
     * @param key Name of the Query component to get timing information on
     * @return a List of durations
     */
    public synchronized List<Long> getTimings(String key) {
        List<Long> timingList = new ArrayList<Long>();
        List<StartEnd> regTime = timings.get(key);
        if (regTime != null) {
            for (StartEnd timing : regTime) {
                timingList.add(timing.endTime - timing.startTime);
            }
        }
        DurationCount qTime = fullTimings.get(key);
        if (qTime != null) {
            final Long duration = qTime.duration;
            for (int i = 0; i < qTime.count; i++) {
                timingList.add(duration);
            }
        }
        return timingList;
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

    private static class StartEnd {
        final long startTime;
        final long endTime;

        public StartEnd(long startTime, long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }
    }

    private static class DurationCount {
        long duration;
        long count;
    }
}

// End QueryTiming.java
