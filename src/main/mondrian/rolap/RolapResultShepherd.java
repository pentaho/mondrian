/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2011-2011 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.server.Execution;
import mondrian.util.Pair;

import java.util.List;
import java.util.concurrent.*;

/**
 * A utility class for {@link RolapConnection}. It specializes in
 * shepherding the creation of RolapResult by running the actual execution
 * on a separate thread from the user thread so we can:
 * <ul>
 * <li>Monitor all executions for timeouts and resource limits as they run
 * in the background</li>
 * <li>Bubble exceptions to the user thread as fast as they happen.</li>
 * <li>Gracefully cancel all SQL statements and cleanup in the background.</li>
 * </ul>
 *
 * @author LBoudreau
 * @version $Id$
 */
class RolapResultShepherd {

    /**
     * An executor service used for both the shepherd thread and the
     * Execution objects.
     */
    private static final ExecutorService executor =
        Util.getExecutorService("mondrian.rolap.RolapResultShepherd$executor");

    /**
     * List of tasks that should be monitored by the shepherd thread.
     */
    private static final List<Pair<FutureTask<Result>, Execution>> tasks =
        new CopyOnWriteArrayList<Pair<FutureTask<Result>,Execution>>();

    /*
     * Fire up the shepherd thread.
     */
    static {
        executor.execute(
            new Runnable() {
                private final int delay =
                    MondrianProperties.instance()
                        .RolapConnectionShepherdThreadPollingInterval.get();
                public void run() {
                    while (true) {
                        for (Pair<FutureTask<Result>, Execution> task
                            : tasks)
                        {
                            if (task.left.isDone()) {
                                continue;
                            }
                            if (task.right.isCancelOrTimeout()) {
                                // First, free the user thread.
                                task.left.cancel(true);
                                // Now try a graceful shutdown of the Execution
                                // instance
                                task.right.cleanStatements(
                                    Execution.State.CANCELED);
                            }
                        }
                        try {
                            Thread.sleep(delay);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
            });
    }

    /**
     * Executes and shepherds the execution of an Execution instance.
     * The shepherd will wrap the Execution instance into a Future object
     * which can be monitored for exceptions. If any are encountered,
     * two things will happen. First, the user thread will be returned and
     * the resulting exception will bubble up. Second, the execution thread
     * will attempt to do a graceful stop of all running SQL statements and
     * release all other resources gracefully in the background.
     * @param execution An Execution instance.
     * @param callable A callable to monitor returning a Result instance.
     * @throws ResourceLimitExceededException if some resource limit specified
     * in the property file was exceeded
     * @throws QueryCanceledException if query was canceled during execution
     * @throws QueryTimeoutException if query exceeded timeout specified in
     * the property file
     * @return A Result object, as supplied by the Callable passed as a
     * parameter.
     */
    static Result shepherdExecution(
        Execution execution,
        Callable<Result> callable)
    {
        // We must wrap this execution into a task that so that we are able
        // to monitor, cancel and detach from it.
        FutureTask<Result> task = new FutureTask<Result>(callable);
        // Register this task with the shepherd thread
        final Pair<FutureTask<Result>, Execution> pair =
            new Pair<FutureTask<Result>, Execution>(
                task,
                execution);
        tasks.add(pair);
        try {
            // Now run it.
            executor.execute(task);
            return task.get();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            // Let the Execution throw whatever it wants to, this way the
            // API contract is respected. The program should in most cases
            // stop here as most exceptions will originate from the Execution
            // instance.
            execution.checkCancelOrTimeout();
            // We must also check for ResourceLimitExceededExceptions,
            // which might be wrapped by an ExecutionException. In order to
            // respect the API contract, we must throw the cause, not the
            // wrapper.
            if (e instanceof ResourceLimitExceededException) {
                throw (ResourceLimitExceededException) e;
            }
            Throwable node = e;
            while (node.getCause() != null && node != node.getCause()) {
                node = node.getCause();
                if (node instanceof ResourceLimitExceededException) {
                    throw (ResourceLimitExceededException) node;
                }
            }
            // Since we got here, this means that the exception was
            // something else. Just wrap/throw.
            throw new MondrianException(e);
        } finally {
            tasks.remove(pair);
        }
    }
}

// End RolapResultShepherd.java

