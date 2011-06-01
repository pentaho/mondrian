/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/

package mondrian.util;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import mondrian.olap.Util;

/**
 * Combining generator is a utility class that takes a list
 * of objects and creates each and every possible combination
 * of those objects.
 * @author LBoudreau
 */
public class CombiningGenerator {

    private final static ExecutorService executor =
        Util.getExecutorService(
            1,
            "mondrian.util.CombiningGenerator$ExecutorThread");
    /**
     * Generates all combinations of a list of objects.
     * @param seed The list of objects to combine.
     * @return A set of all possible combinations.
     */
    public static <E> Set<Set<E>> generate(Collection<E> seed)
    {
        return generate(seed, 1);
    }

    /**
     * Generates all combinations of a list of objects.
     * @param seed The list of objects to combine.
     * @param minLength The minimum number of elements
     * per combination in order for a combination
     * to be valid.
     * @return A set of all possible combinations.
     */
    public static <E> Set<Set<E>> generate(
            Collection<E> seed,
            int minLength)
    {
        final Set<Set<E>> result =
            Collections.synchronizedSet(
                new HashSet<Set<E>>());
        final List<FutureTask<Boolean>> tasks =
            Collections.synchronizedList(
                new ArrayList<FutureTask<Boolean>>());
        final List<E> seedList =
            Collections.synchronizedList(
                new ArrayList<E>());
        final AtomicInteger countLatch =
            new AtomicInteger(1);

        seedList.addAll(seed);

        generateRecursive(countLatch, tasks, seedList, result, minLength);

        while (true) {
            if (countLatch.get() == 0) {
                return result;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // no op .
            }
        }
    }

    private static <E> void generateRecursive(
        final AtomicInteger countLatch,
        final List<FutureTask<Boolean>> tasks,
        final List<E> seed,
        final Set<Set<E>> combined,
        final int minLength)
    {
        if (seed.size() < minLength) {
            countLatch.getAndDecrement();
            return;
        }
        combined.add(new HashSet<E>(seed));
        if (seed.size() == 1)
        {
            combined.add(new HashSet<E>(seed));
            countLatch.getAndDecrement();
            return;
        }
        for (int i = 0; i < seed.size(); i++) {
            final int position = i;
            final FutureTask<Boolean> task = new FutureTask<Boolean>(
                new Callable<Boolean>() {
                    public Boolean call() throws Exception {
                        List<E> subList =
                            new ArrayList<E>(seed.subList(0, position));
                        subList.addAll(
                            seed.subList(position + 1, seed.size()));
                        generateRecursive(
                            countLatch, tasks, subList, combined, minLength);
                        return true;
                    }
                });
            countLatch.getAndIncrement();
            tasks.add(task);
            executor.execute(task);
        }
        countLatch.getAndDecrement();
    }

    public static void main(String[] args) {
        List<Object> seed = new ArrayList<Object>();
        for (int i = 0; i < 8; i++) {
            seed.add(String.valueOf(i));
        }
        Set<Set<Object>> result = CombiningGenerator.generate(seed, 1);
        for (Set<Object> i : result) {
            for (Object o : i) {
                System.out.print("|");
                System.out.print(String.valueOf(o));
            }
            System.out.println("|");
        }
    }
}
// End CombiningGenerator.java