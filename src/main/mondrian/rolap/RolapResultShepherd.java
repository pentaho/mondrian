/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2011-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.*;
import mondrian.resource.MondrianResource;
import mondrian.server.Execution;
import mondrian.util.Pair;

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
    private final ExecutorService executor =
        Util.getExecutorService(
            MondrianProperties.instance()
                .RolapConnectionShepherdNbThreads.get(),
            0, 1,
            "mondrian.rolap.RolapResultShepherd$executor",
            new RejectedExecutionHandler() {
                public void rejectedExecution(
                    Runnable r,
                    ThreadPoolExecutor executor)
                {
                    throw MondrianResource.instance()
                        .QueryLimitReached.ex();
                }
            });

    /**
     * List of tasks that should be monitored by the shepherd thread.
     */
    private static final List<Pair<FutureTask<Result>, Execution>> tasks =
        new CopyOnWriteArrayList<Pair<FutureTask<Result>,Execution>>();

    private final Timer timer =
        Util.newTimer("mondrian.rolap.RolapResultShepherd#timer", true);

    public RolapResultShepherd() {
        timer.scheduleAtFixedRate(
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
                        task.left.cancel(true);
                        // The cleanup operation can be done async.
                        // Let's not interrupt this task.
                        executor.submit(
                            new Runnable() {
                                public void run() {
                                    // Now cleanup the statement on
                                    // this thread.
                                    task.right.cancelSqlStatements();
                            }
                        });
                    }
                }
            }
        },
        MondrianProperties.instance()
            .RolapConnectionShepherdThreadPollingInterval.get(),
        MondrianProperties.instance()
            .RolapConnectionShepherdThreadPollingInterval.get());
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
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
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
    }
}

// End RolapResultShepherd.java

