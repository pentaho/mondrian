/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.rolap;

import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.server.Execution;
import mondrian.util.Pair;

import org.eigenbase.util.property.IntegerProperty;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
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
 */
public class RolapResultShepherd {

    /**
     * An executor service used for both the shepherd thread and the
     * Execution objects.
     */
    private final ExecutorService executor;

    /**
     * List of tasks that should be monitored by the shepherd thread.
     */
    private final List<Pair<FutureTask<Result>, Execution>> tasks =
        new CopyOnWriteArrayList<Pair<FutureTask<Result>,Execution>>();

    private final Timer timer =
        Util.newTimer("mondrian.rolap.RolapResultShepherd#timer", true);

    public RolapResultShepherd() {
        final IntegerProperty property =
            MondrianProperties.instance().RolapConnectionShepherdNbThreads;
        final int maximumPoolSize = property.get();
        executor =
            Util.getExecutorService(
                 // We use the same value for coreSize and maxSize
                // because that's the behavior we want. All extra
                // tasks will be put on an unbounded queue.
                maximumPoolSize,
                maximumPoolSize,
                1,
                "mondrian.rolap.RolapResultShepherd$executor",
                new RejectedExecutionHandler() {
                    public void rejectedExecution(
                        Runnable r,
                        ThreadPoolExecutor executor)
                    {
                        throw MondrianResource.instance().QueryLimitReached.ex(
                            maximumPoolSize,
                            property.getPath());
                    }
                });
        final Pair<Long, TimeUnit> interval =
            Util.parseInterval(
                String.valueOf(
                    MondrianProperties.instance()
                        .RolapConnectionShepherdThreadPollingInterval.get()),
                TimeUnit.MILLISECONDS);
        long period = interval.right.toMillis(interval.left);
        timer.schedule(
            new TimerTask() {
                public void run() {
                    for (final Pair<FutureTask<Result>, Execution> task
                        : tasks)
                    {
                        if (task.left.isDone()) {
                            tasks.remove(task);
                            continue;
                        }
                        if (task.right.isCancelOrTimeout()) {
                            // Remove it from the list so that we know
                            // it was cleaned once.
                            tasks.remove(task);

                            // Cancel the FutureTask for which
                            // the user thread awaits. The user
                            // thread will call
                            // Execution.checkCancelOrTimeout
                            // later and take care of sending
                            // an exception on the user thread.
                            task.left.cancel(false);
                        }
                    }
                }
            },
            period,
            period);
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
    public Result shepherdExecution(
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
        } catch (Throwable e) {
            // Make sure to clean up pending SQL queries.
            execution.cancelSqlStatements();

            // Make sure to propagate the interruption flag.
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            // Unwrap any java.concurrent wrappers.
            Throwable node = e;
            if (e instanceof ExecutionException) {
                ExecutionException executionException = (ExecutionException) e;
                node = executionException.getCause();
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
            final ResourceLimitExceededException t =
                Util.getMatchingCause(
                    node, ResourceLimitExceededException.class);
            if (t != null) {
                throw t;
            }

            // Check for Mondrian exceptions in the exception chain.
            // we can throw these back as-is.
            final MondrianException m =
                Util.getMatchingCause(
                    node, MondrianException.class);
            if (m != null) {
                // Throw that.
                throw m;
            }

            // Since we got here, this means that the exception was
            // something else. Just wrap/throw.
            if (node instanceof RuntimeException) {
                throw (RuntimeException) node;
            } else if (node instanceof Error) {
                throw (Error) node;
            } else {
                throw new MondrianException(node);
            }
        }
    }

    public void shutdown() {
        this.timer.cancel();
        this.executor.shutdown();
        this.tasks.clear();
    }
}

// End RolapResultShepherd.java

