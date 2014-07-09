/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2001-2005 Julian Hyde
// Copyright (C) 2005-2014 Pentaho and others
// All Rights Reserved.
*/
package mondrian.olap;

import mondrian.mdx.*;
import mondrian.olap.fun.FunUtil;
import mondrian.olap.fun.Resolver;
import mondrian.olap.type.Type;
import mondrian.resource.MondrianResource;
import mondrian.rolap.*;
import mondrian.spi.*;
import mondrian.util.*;

import org.apache.commons.collections.Predicate;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import org.eigenbase.xom.XOMUtil;

import org.olap4j.impl.*;
import org.olap4j.mdx.*;
import org.olap4j.metadata.NamedList;

import java.io.*;
import java.lang.ref.Reference;
import java.lang.reflect.*;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.sql.Connection;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility functions used throughout mondrian. All methods are static.
 *
 * @author jhyde
 * @since 6 August, 2001
 */
public class Util extends XOMUtil {

    public static final String nl = System.getProperty("line.separator");

    private static final Logger LOGGER = Logger.getLogger(Util.class);

    /**
     * Placeholder which indicates a value NULL.
     */
    public static final Object nullValue = new Double(FunUtil.DoubleNull);

    /**
     * Placeholder which indicates an EMPTY value.
     */
    public static final Object EmptyValue = new Double(FunUtil.DoubleEmpty);

    /**
     * Cumulative time spent accessing the database.
     */
    private static long databaseMillis = 0;

    /**
     * Random number generator to provide seed for other random number
     * generators.
     */
    private static final Random metaRandom =
            createRandom(MondrianProperties.instance().TestSeed.get());

    /** Unique id for this JVM instance. Part of a key that ensures that if
     * two JVMs in the same cluster have a data-source with the same
     * identity-hash-code, they will be treated as different data-sources,
     * and therefore caches will not be incorrectly shared. */
    public static final UUID JVM_INSTANCE_UUID = UUID.randomUUID();

    /**
     * Whether we are running a version of Java before 1.5.
     *
     * <p>If (but not only if) this variable is true, {@link #Retrowoven} will
     * also be true.
     */
    public static final boolean PreJdk15 =
        System.getProperty("java.version").startsWith("1.4");

    /**
     * Whether we are running a version of Java before 1.6.
     */
    public static final boolean PreJdk16 =
        PreJdk15
        || System.getProperty("java.version").startsWith("1.5");

    /**
     * Whether we are running a version of Java before 1.7.
     */
    public static final boolean PreJdk17 =
        PreJdk16
        || System.getProperty("java.version").startsWith("1.6");

    /**
     * Whether this is an IBM JVM.
     */
    public static final boolean IBM_JVM =
        System.getProperties().getProperty("java.vendor").equals(
            "IBM Corporation");

    /**
     * What version of JDBC?
     * Returns:<ul>
     *     <li>0x0401 in JDK 1.7 and higher</li>
     *     <li>0x0400 in JDK 1.6</li>
     *     <li>0x0300 otherwise</li>
     * </ul>
     */
    public static final int JdbcVersion =
        System.getProperty("java.version").compareTo("1.7") >= 0
            ? 0x0401
            : System.getProperty("java.version").compareTo("1.6") >= 0
            ? 0x0400
            : 0x0300;

    /**
     * Whether the code base has re-engineered using retroweaver.
     * If this is the case, some functionality is not available, but a lot of
     * things are available via {@link mondrian.util.UtilCompatible}.
     * Retroweaver has some problems involving {@link java.util.EnumSet}.
     */
    public static final boolean Retrowoven =
        Access.class.getSuperclass().getName().equals(
            "net.sourceforge.retroweaver.runtime.java.lang.Enum");

    private static Boolean HAVE_SCRIPTING;

    private static final UtilCompatible compatible;

    /**
     * Flag to control expensive debugging. (More expensive than merely
     * enabling assertions: as we know, a lot of people run with assertions
     * enabled.)
     */
    public static final boolean DEBUG = false;

    public static final ComparableEmptyList COMPARABLE_EMPTY_LIST =
        new ComparableEmptyList();

    public static final Lazy<VirtualFileHandler> VIRTUAL_FILE_HANDLER =
        new Lazy<VirtualFileHandler>(VirtualFileHandler.FACTORY);

    static {
        String className;
        if (PreJdk15 || Retrowoven) {
            className = "mondrian.util.UtilCompatibleJdk14";
        } else if (PreJdk16) {
            className = "mondrian.util.UtilCompatibleJdk15";
        } else if (PreJdk17) {
            className = "mondrian.util.UtilCompatibleJdk16";
        } else {
            className = "mondrian.util.UtilCompatibleJdk17";
        }
        compatible = ClassResolver.INSTANCE.instantiateSafe(className);
    }

    private static boolean deduceHaveScripting() {
        // Figure out whether scripting is available.
        // We know that scripting only exists in JDK 1.6 and later.
        // But even then, if certain JARs are not on path, the JavaScript
        // engine may not be available. OpenJDK 1.7 has this problem.
        if (PreJdk16) {
            return false;
        }
        try {
            //noinspection unchecked
            final Function0<Boolean> function =
                compatible.compileScript(
                    Function0.class,
                    "function apply() { return true; }",
                    "JavaScript");
            return function.apply();
        } catch (MondrianException e) {
            Util.discard(e);
        }
        return false;
    }

    /**
     * Returns whether scripting is available.
     */
    public static synchronized boolean haveScripting() {
        if (HAVE_SCRIPTING == null) {
            // Figure out whether scripting is available.
            // We know that scripting only exists in JDK 1.6 and later.
            // But even then, if certain JARs are not on path, the JavaScript
            // engine may not be available. OpenJDK 1.7 has this problem.
            HAVE_SCRIPTING = deduceHaveScripting();
        }
        return HAVE_SCRIPTING;
    }

    public static boolean isNull(Object o) {
        return o == null || o == nullValue;
    }

    /**
     * Returns whether a list is strictly sorted.
     *
     * @param list List
     * @return whether list is sorted
     */
    public static <T> boolean isSorted(List<T> list) {
        T prev = null;
        for (T t : list) {
            if (prev != null
                && ((Comparable<T>) prev).compareTo(t) >= 0)
            {
                return false;
            }
            prev = t;
        }
        return true;
    }

    /**
     * Parses a string and returns a SHA-256 checksum of it.
     *
     * @param value The source string to parse.
     * @return A checksum of the source string.
     */
    public static byte[] digestSha256(String value) {
        final MessageDigest algorithm;
        try {
            algorithm = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return algorithm.digest(value.getBytes());
    }

    /**
     * Creates an MD5 hash of a String.
     *
     * @param value String to create one way hash upon.
     * @return MD5 hash.
     */
    public static byte[] digestMd5(final String value) {
        final MessageDigest algorithm;
        try {
            algorithm = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        return algorithm.digest(value.getBytes());
    }

    /**
     * Creates an {@link ExecutorService} object backed by a thread pool.
     * @param maximumPoolSize Maximum number of concurrent
     * threads.
     * @param corePoolSize Minimum number of concurrent
     * threads to maintain in the pool, even if they are
     * idle.
     * @param keepAliveTime Time, in seconds, for which to
     * keep alive unused threads.
     * @param name The name of the threads.
     * @param rejectionPolicy The rejection policy to enforce.
     * @return An executor service preconfigured.
     */
    public static ExecutorService getExecutorService(
        int maximumPoolSize,
        int corePoolSize,
        long keepAliveTime,
        final String name,
        RejectedExecutionHandler rejectionPolicy)
    {
        if (Util.PreJdk16) {
            // On JDK1.5, if you specify corePoolSize=0, nothing gets executed.
            // Bummer.
            corePoolSize = Math.max(corePoolSize, 1);
        }

        // We must create a factory where the threads
        // have the right name and are marked as daemon threads.
        final ThreadFactory factory =
            new ThreadFactory() {
                private final AtomicInteger counter = new AtomicInteger(0);
                public Thread newThread(Runnable r) {
                    final Thread t =
                        Executors.defaultThreadFactory().newThread(r);
                    t.setDaemon(true);
                    t.setName(name + '_' + counter.incrementAndGet());
                    return t;
                }
            };

        // Ok, create the executor
        final ThreadPoolExecutor executor =
            new ThreadPoolExecutor(
                corePoolSize,
                maximumPoolSize > 0
                    ? maximumPoolSize
                    : Integer.MAX_VALUE,
                keepAliveTime,
                TimeUnit.SECONDS,
                // we use a sync queue. any other type of queue
                // will prevent the tasks from running concurrently
                // because the executors API requires blocking queues.
                // Important to pass true here. This makes the
                // order of tasks deterministic.
                // TODO Write a non-blocking queue which implements
                // the blocking queue API so we can pass that to the
                // executor.
                new SynchronousQueue<Runnable>(true),
                factory);

        // Set the rejection policy if required.
        if (rejectionPolicy != null) {
            executor.setRejectedExecutionHandler(
                rejectionPolicy);
        }

        // Done
        return executor;
    }

    /**
     * Creates an {@link ScheduledExecutorService} object backed by a
     * thread pool with a fixed number of threads..
     * @param maxNbThreads Maximum number of concurrent
     * threads.
     * @param name The name of the threads.
     * @return An scheduled executor service preconfigured.
     */
    public static ScheduledExecutorService getScheduledExecutorService(
        final int maxNbThreads,
        final String name)
    {
        return Executors.newScheduledThreadPool(
            maxNbThreads,
            new ThreadFactory() {
                final AtomicInteger counter = new AtomicInteger(0);

                public Thread newThread(Runnable r) {
                    final Thread thread =
                        Executors.defaultThreadFactory().newThread(r);
                    thread.setDaemon(true);
                    thread.setName(name + '_' + counter.incrementAndGet());
                    return thread;
                }
            }
        );
    }

    /**
     * Distributes and executes a list of tasks via an executor
     * service. This function will only return if one of the two
     * following things occur:
     * <ul>
     * <li>all tasks have finished running</li>
     * <li><code>breakAtFirstNonNull</code> is set to true and
     * one of the tasks has returned a non null value.</li>
     * </ul>
     * @param <E>
     * @param tasks List of tasks to run.
     * @param executor The executor service to use.
     * @param breakAtFirstNonNull Whether or not to stop executing
     * the tasks if one of them returns a non null value. Useful
     * when scanning a list of items for the first match found.
     * @return If <code>breakAtFirstNonNull</code> is true, this
     * function returns the first non null result given by the first
     * task to complete. Returns null otherwise.
     */
    public static <E> E executeDistributedTasks(
        List<Callable<E>> tasks,
        ExecutorService executor,
        boolean breakAtFirstNonNull)
    {
        final List<Future<E>> tasksList =
            new ArrayList<Future<E>>();
        final CountDownLatch latch =
            new CountDownLatch(tasks.size());
        try {
            for (final Callable<E> call : tasks) {
                tasksList.add(
                    executor.submit(
                        new Callable<E>() {
                            public E call() throws Exception {
                                E result = call.call();
                                latch.countDown();
                                return result;
                            }
                        }));
            }

            E result = null;
            taskLoop:
            while (true) {
                if (breakAtFirstNonNull) {
                    for (Future<E> task : tasksList) {
                        if (task.isDone()) {
                            E taskResult = null;
                            try {
                                taskResult = task.get();
                            } catch (InterruptedException e) {
                                throw new MondrianException(e);
                            } catch (ExecutionException e) {
                                throw new MondrianException(e);
                            }
                            if (taskResult != null) {
                                result = taskResult;
                                break taskLoop;
                            }
                        }
                    }
                }
                // Break anyway if all tasks are completed.
                if (latch.getCount() == 0) {
                    break taskLoop;
                }
                // Sleep for some time as not all tasks seem completed.
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new MondrianException(e);
                }
            }
            return result;
        } finally {
            // Make double sure all tasks are killed
            for (Future<E> task : tasksList) {
                task.cancel(true);
            }
        }
    }

    /**
     * Encodes string for MDX (escapes ] as ]] inside a name).
     */
    public static String mdxEncodeString(String st) {
        StringBuilder retString = new StringBuilder(st.length() + 20);
        for (int i = 0; i < st.length(); i++) {
            char c = st.charAt(i);
            if ((c == ']')
                && ((i + 1) < st.length())
                && (st.charAt(i + 1) != '.'))
            {
                retString.append(']'); // escaping character
            }
            retString.append(c);
        }
        return retString.toString();
    }

    /**
     * Converts a string into a double-quoted string.
     */
    public static String quoteForMdx(String val) {
        StringBuilder buf = new StringBuilder(val.length() + 20);
        quoteForMdx(buf, val);
        return buf.toString();
    }

    /**
     * Appends a double-quoted string to a string builder.
     */
    public static StringBuilder quoteForMdx(StringBuilder buf, String val) {
        buf.append("\"");
        String s0 = replace(val, "\"", "\"\"");
        buf.append(s0);
        buf.append("\"");
        return buf;
    }

    /**
     * Return string quoted in [...].  For example, "San Francisco" becomes
     * "[San Francisco]"; "a [bracketed] string" becomes
     * "[a [bracketed]] string]".
     */
    public static String quoteMdxIdentifier(String id) {
        StringBuilder buf = new StringBuilder(id.length() + 20);
        quoteMdxIdentifier(id, buf);
        return buf.toString();
    }

    public static void quoteMdxIdentifier(String id, StringBuilder buf) {
        buf.append('[');
        int start = buf.length();
        buf.append(id);
        replace(buf, start, "]", "]]");
        buf.append(']');
    }

    /**
     * Return identifiers quoted in [...].[...].  For example, {"Store", "USA",
     * "California"} becomes "[Store].[USA].[California]".
     */
    public static String quoteMdxIdentifier(List<Id.Segment> ids) {
        StringBuilder sb = new StringBuilder(64);
        quoteMdxIdentifier(ids, sb);
        return sb.toString();
    }

    public static void quoteMdxIdentifier(
        List<Id.Segment> ids,
        StringBuilder sb)
    {
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) {
                sb.append('.');
            }
            ids.get(i).toString(sb);
        }
    }

    /**
     * Quotes a string literal for Java or JavaScript.
     *
     * @param s Unquoted literal
     * @return Quoted string literal
     */
    public static String quoteJavaString(String s) {
        return s == null
            ? "null"
            : "\""
              + s.replaceAll("\\\\", "\\\\\\\\")
                .replaceAll("\\\"", "\\\\\"")
              + "\"";
    }

    /**
     * Returns true if two objects are equal, or are both null.
     *
     * @param s First object
     * @param t Second object
     * @return Whether objects are equal or both null
     */
    public static boolean equals(Object s, Object t) {
        if (s == t) {
            return true;
        }
        if (s == null || t == null) {
            return false;
        }
        return s.equals(t);
    }

    /**
     * Returns true if two strings are equal, or are both null.
     *
     * <p>The result is not affected by
     * {@link MondrianProperties#CaseSensitive the case sensitive option}; if
     * you wish to compare names, use {@link #equalName(String, String)}.
     */
    public static boolean equals(String s, String t) {
        return equals((Object) s, (Object) t);
    }

    /**
     * Returns whether two names are equal.
     * Takes into account the
     * {@link MondrianProperties#CaseSensitive case sensitive option}.
     * Names may be null.
     */
    public static boolean equalName(String s, String t) {
        if (s == null) {
            return t == null;
        }
        boolean caseSensitive =
            MondrianProperties.instance().CaseSensitive.get();
        return caseSensitive ? s.equals(t) : s.equalsIgnoreCase(t);
    }

    /**
     * Tests two strings for equality, optionally ignoring case.
     *
     * @param s First string
     * @param t Second string
     * @param matchCase Whether to perform case-sensitive match
     * @return Whether strings are equal
     */
    public static boolean equal(String s, String t, boolean matchCase) {
        return matchCase ? s.equals(t) : s.equalsIgnoreCase(t);
    }

    public static String toUpperCase(String s) {
        return s == null ? null : s.toUpperCase();
    }

    /**
     * Compares two names.  if case sensitive flag is false,
     * apply finer grain difference with case sensitive
     * Takes into account the {@link MondrianProperties#CaseSensitive case
     * sensitive option}.
     * Names must not be null.
     */
    public static int caseSensitiveCompareName(String s, String t) {
        boolean caseSensitive =
            MondrianProperties.instance().CaseSensitive.get();
        if (caseSensitive) {
            return s.compareTo(t);
        } else {
            int v = s.compareToIgnoreCase(t);
            // if ignore case returns 0 compare in a case sensitive manner
            // this was introduced to solve an issue with Member.equals()
            // and Member.compareTo() not agreeing with each other
            return v == 0 ? s.compareTo(t) : v;
        }
    }

    /**
     * Compares two names.
     * Takes into account the {@link MondrianProperties#CaseSensitive case
     * sensitive option}.
     * Names must not be null.
     */
    public static int compareName(String s, String t) {
        boolean caseSensitive =
            MondrianProperties.instance().CaseSensitive.get();
        return caseSensitive ? s.compareTo(t) : s.compareToIgnoreCase(t);
    }

    /**
     * Generates a normalized form of a name, for use as a key into a map.
     * Returns the upper case name if
     * {@link MondrianProperties#CaseSensitive} is true, the name unchanged
     * otherwise.
     */
    public static String normalizeName(String s) {
        return MondrianProperties.instance().CaseSensitive.get()
            ? s
            : s.toUpperCase();
    }

    /**
     * Returns the result of ((Comparable) k1).compareTo(k2), with
     * special-casing for the fact that Boolean only became
     * comparable in JDK 1.5.
     *
     * @see Comparable#compareTo
     */
    public static int compareKey(Object k1, Object k2) {
        if (k1 instanceof Boolean) {
            // Luckily, "F" comes before "T" in the alphabet.
            k1 = k1.toString();
            k2 = k2.toString();
        } else if (k1 instanceof List) {
            return compareLists((List) k1, (List) k2);
        }
        return ((Comparable) k1).compareTo(k2);
    }

    private static int compareLists(List list0, List list1) {
        final int size0 = list0.size();
        final int size1 = list1.size();
        int c = compare(size0, size1);
        if (c != 0) {
            return c;
        }
        for (int i = 0; i < size0; i++) {
            Object o0 = list0.get(i);
            Object o1 = list1.get(i);
            c = compareKey(o0, o1);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

    /**
     * Compares two integers using the same algorithm as
     * {@link Integer#compareTo(Integer)}.
     *
     * @param i0 First integer
     * @param i1 Second integer
     * @return Comparison of integers
     */
    public static int compare(int i0, int i1) {
        return i0 < i1 ? -1 : (i0 == i1 ? 0 : 1);
    }

    /**
     * Compares two comparables, handling null values.
     *
     * @param t0 First value
     * @param t1 Second value
     * @param <T> Value type
     * @return Comparison, per {@link Comparable#compareTo(Object)}
     */
    public static <T extends Comparable<T>> int compare(T t0, T t1) {
        if (t0 == null) {
            if (t1 == null) {
                return 0;
            } else {
                // null is less than anything else
                return -1;
            }
        } else if (t1 == null) {
            return 1;
        } else {
            return RolapUtil.ROLAP_COMPARATOR.compare(t0, t1);
        }
    }

    /**
     * Returns a string with every occurrence of a seek string replaced with
     * another.
     */
    public static String replace(String s, String find, String replace) {
        // let's be optimistic
        int found = s.indexOf(find);
        if (found == -1) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s.length() + 20);
        int start = 0;
        char[] chars = s.toCharArray();
        final int step = find.length();
        if (step == 0) {
            // Special case where find is "".
            sb.append(s);
            replace(sb, 0, find, replace);
        } else {
            for (;;) {
                sb.append(chars, start, found - start);
                if (found == s.length()) {
                    break;
                }
                sb.append(replace);
                start = found + step;
                found = s.indexOf(find, start);
                if (found == -1) {
                    found = s.length();
                }
            }
        }
        return sb.toString();
    }

    /**
     * Replaces all occurrences of a string in a buffer with another.
     *
     * @param buf String buffer to act on
     * @param start Ordinal within <code>find</code> to start searching
     * @param find String to find
     * @param replace String to replace it with
     * @return The string buffer
     */
    public static StringBuilder replace(
        StringBuilder buf,
        int start,
        String find,
        String replace)
    {
        // Search and replace from the end towards the start, to avoid O(n ^ 2)
        // copying if the string occurs very commonly.
        int findLength = find.length();
        if (findLength == 0) {
            // Special case where the seek string is empty.
            for (int j = buf.length(); j >= 0; --j) {
                buf.insert(j, replace);
            }
            return buf;
        }
        int k = buf.length();
        while (k > 0) {
            int i = buf.lastIndexOf(find, k);
            if (i < start) {
                break;
            }
            buf.replace(i, i + find.length(), replace);
            // Step back far enough to ensure that the beginning of the section
            // we just replaced does not cause a match.
            k = i - findLength;
        }
        return buf;
    }

    /**
     * Parses an MDX identifier such as <code>[Foo].[Bar].Baz.&Key&Key2</code>
     * and returns the result as a list of segments.
     *
     * @param s MDX identifier
     * @return List of segments
     */
    public static List<Id.Segment> parseIdentifier(String s)  {
        return convert(
            org.olap4j.impl.IdentifierParser.parseIdentifier(s));
    }

    /**
     * Converts an array of name parts {"part1", "part2"} into a single string
     * "[part1].[part2]". If the names contain "]" they are escaped as "]]".
     */
    public static String implode(List<Id.Segment> names) {
        StringBuilder sb = new StringBuilder(64);
        for (int i = 0; i < names.size(); i++) {
            if (i > 0) {
                sb.append(".");
            }
            // FIXME: should be:
            //   names.get(i).toString(sb);
            // but that causes some tests to fail
            Id.Segment segment = names.get(i);
            switch (segment.getQuoting()) {
            case UNQUOTED:
                segment = new Id.NameSegment(((Id.NameSegment) segment).name);
            }
            segment.toString(sb);
        }
        return sb.toString();
    }

    public static String makeFqName(String name) {
        return quoteMdxIdentifier(name);
    }

    public static String makeFqName(OlapElement parent, String name) {
        if (parent == null) {
            return Util.quoteMdxIdentifier(name);
        } else {
            StringBuilder buf = new StringBuilder(64);
            buf.append(parent.getUniqueName());
            buf.append('.');
            Util.quoteMdxIdentifier(name, buf);
            return buf.toString();
        }
    }

    public static String makeFqName(String parentUniqueName, String name) {
        if (parentUniqueName == null) {
            return quoteMdxIdentifier(name);
        } else {
            StringBuilder buf = new StringBuilder(64);
            buf.append(parentUniqueName);
            buf.append('.');
            Util.quoteMdxIdentifier(name, buf);
            return buf.toString();
        }
    }

    public static OlapElement lookup(Query q, List<Id.Segment> nameParts) {
        final Exp exp = lookup(q, nameParts, false);
        if (exp instanceof MemberExpr) {
            MemberExpr memberExpr = (MemberExpr) exp;
            return memberExpr.getMember();
        } else if (exp instanceof LevelExpr) {
            LevelExpr levelExpr = (LevelExpr) exp;
            return levelExpr.getLevel();
        } else if (exp instanceof HierarchyExpr) {
            HierarchyExpr hierarchyExpr = (HierarchyExpr) exp;
            return hierarchyExpr.getHierarchy();
        } else if (exp instanceof DimensionExpr) {
            DimensionExpr dimensionExpr = (DimensionExpr) exp;
            return dimensionExpr.getDimension();
        } else {
            throw Util.newInternal("Not an olap element: " + exp);
        }
    }

    /**
     * Converts an identifier into an expression by resolving its parts into
     * an OLAP object (dimension, hierarchy, level or member) within the
     * context of a query.
     *
     * <p>If <code>allowProp</code> is true, also allows property references
     * from valid members, for example
     * <code>[Measures].[Unit Sales].FORMATTED_VALUE</code>.
     * In this case, the result will be a {@link mondrian.mdx.ResolvedFunCall}.
     *
     * @param q Query expression belongs to
     * @param nameParts Parts of the identifier
     * @param allowProp Whether to allow property references
     * @return OLAP object or property reference
     */
    public static Exp lookup(
        Query q,
        List<Id.Segment> nameParts,
        boolean allowProp)
    {
        return lookup(q, q.getSchemaReader(true), nameParts, allowProp);
    }

    /**
     * Converts an identifier into an expression by resolving its parts into
     * an OLAP object (dimension, hierarchy, level or member) within the
     * context of a query.
     *
     * <p>If <code>allowProp</code> is true, also allows property references
     * from valid members, for example
     * <code>[Measures].[Unit Sales].FORMATTED_VALUE</code>.
     * In this case, the result will be a {@link ResolvedFunCall}.
     *
     * @param q Query expression belongs to
     * @param schemaReader Schema reader
     * @param segments Parts of the identifier
     * @param allowProp Whether to allow property references
     * @return OLAP object or property reference
     */
    public static Exp lookup(
        Query q,
        SchemaReader schemaReader,
        List<Id.Segment> segments,
        boolean allowProp)
    {
        // First, look for a calculated member defined in the query.
        final String fullName = quoteMdxIdentifier(segments);
        // Look for any kind of object (member, level, hierarchy,
        // dimension) in the cube. Use a schema reader without restrictions.
        final SchemaReader schemaReaderSansAc =
            schemaReader.withoutAccessControl().withLocus();
        final Cube cube = q.getCube();
        OlapElement olapElement =
            schemaReaderSansAc.lookupCompound(
                cube, segments, false, Category.Unknown);
        if (olapElement != null) {
            Role role = schemaReader.getRole();
            if (!role.canAccess(olapElement)) {
                olapElement = null;
            }
            if (olapElement instanceof Member) {
                olapElement =
                    schemaReader.substitute((Member) olapElement);
            }
        }
        if (olapElement == null) {
            if (allowProp && segments.size() > 1) {
                List<Id.Segment> segmentsButOne =
                    segments.subList(0, segments.size() - 1);
                final Id.Segment lastSegment = last(segments);
                final String propertyName =
                    lastSegment instanceof Id.NameSegment
                        ? ((Id.NameSegment) lastSegment).getName()
                        : null;
                final Member member =
                    (Member) schemaReaderSansAc.lookupCompound(
                        cube, segmentsButOne, false, Category.Member);
                if (member != null
                    && propertyName != null
                    && isValidProperty(propertyName, member.getLevel()))
                {
                    return new UnresolvedFunCall(
                        propertyName, Syntax.Property, new Exp[] {
                            createExpr(member)});
                }
                final Level level =
                    (Level) schemaReaderSansAc.lookupCompound(
                        cube, segmentsButOne, false, Category.Level);
                if (level != null
                    && propertyName != null
                    && isValidProperty(propertyName, level))
                {
                    return new UnresolvedFunCall(
                        propertyName, Syntax.Property, new Exp[] {
                            createExpr(level)});
                }
            }
            // if we're in the middle of loading the schema, the property has
            // been set to ignore invalid members, and the member is
            // non-existent, return the null member corresponding to the
            // hierarchy of the element we're looking for; locate the
            // hierarchy by incrementally truncating the name of the element
            if (q.ignoreInvalidMembers()) {
                int nameLen = segments.size() - 1;
                olapElement = null;
                while (nameLen > 0 && olapElement == null) {
                    List<Id.Segment> partialName =
                        segments.subList(0, nameLen);
                    olapElement = schemaReaderSansAc.lookupCompound(
                        cube, partialName, false, Category.Unknown);
                    nameLen--;
                }
                if (olapElement != null) {
                    olapElement = olapElement.getHierarchy().getNullMember();
                } else {
                    throw MondrianResource.instance().MdxChildObjectNotFound.ex(
                        fullName, cube.getQualifiedName());
                }
            } else {
                throw MondrianResource.instance().MdxChildObjectNotFound.ex(
                    fullName, cube.getQualifiedName());
            }
        }
        // keep track of any measure members referenced; these will be used
        // later to determine if cross joins on virtual cubes can be
        // processed natively
        q.addMeasuresMembers(olapElement);
        return createExpr(olapElement);
    }

    /**
     * Looks up a cube in a schema reader.
     *
     * @param cubeName Cube name
     * @param fail Whether to fail if not found.
     * @return Cube, or null if not found
     */
    static Cube lookupCube(
        SchemaReader schemaReader,
        String cubeName,
        boolean fail)
    {
        for (Cube cube : schemaReader.getCubes()) {
            if (Util.compareName(cube.getName(), cubeName) == 0) {
                return cube;
            }
        }
        if (fail) {
            throw MondrianResource.instance().MdxCubeNotFound.ex(cubeName);
        }
        return null;
    }

    /**
     * Converts an olap element (dimension, hierarchy, level or member) into
     * an expression representing a usage of that element in an MDX statement.
     */
    public static Exp createExpr(OlapElement element)
    {
        if (element instanceof Member) {
            Member member = (Member) element;
            return new MemberExpr(member);
        } else if (element instanceof Level) {
            Level level = (Level) element;
            return new LevelExpr(level);
        } else if (element instanceof Hierarchy) {
            Hierarchy hierarchy = (Hierarchy) element;
            return new HierarchyExpr(hierarchy);
        } else if (element instanceof Dimension) {
            Dimension dimension = (Dimension) element;
            return new DimensionExpr(dimension);
        } else if (element instanceof NamedSet) {
            NamedSet namedSet = (NamedSet) element;
            return new NamedSetExpr(namedSet);
        } else {
            throw Util.newInternal("Unexpected element type: " + element);
        }
    }

    public static Member lookupHierarchyRootMember(
        SchemaReader reader, Hierarchy hierarchy, Id.NameSegment memberName)
    {
        return lookupHierarchyRootMember(
            reader, hierarchy, memberName, MatchType.EXACT);
    }

    /**
     * Finds a root member of a hierarchy with a given name.
     *
     * @param hierarchy Hierarchy
     * @param memberName Name of root member
     * @return Member, or null if not found
     */
    public static Member lookupHierarchyRootMember(
        SchemaReader reader,
        Hierarchy hierarchy,
        Id.NameSegment memberName,
        MatchType matchType)
    {
        // Lookup member at first level.
        //
        // Don't use access control. Suppose we cannot see the 'nation' level,
        // we still want to be able to resolve '[Customer].[USA].[CA]'.
        List<Member> rootMembers = reader.getHierarchyRootMembers(hierarchy);

        // if doing an inexact search on a non-all hierarchy, create
        // a member corresponding to the name we're searching for so
        // we can use it in a hierarchical search
        Member searchMember = null;
        if (!matchType.isExact()
            && !hierarchy.hasAll()
            && !rootMembers.isEmpty())
        {
            searchMember =
                hierarchy.createMember(
                    null,
                    rootMembers.get(0).getLevel(),
                    memberName.name,
                    null);
        }

        int bestMatch = -1;
        int k = -1;
        for (Member rootMember : rootMembers) {
            ++k;
            int rc;
            // when searching on the ALL hierarchy, match must be exact
            if (matchType.isExact() || hierarchy.hasAll()) {
                rc = rootMember.getName().compareToIgnoreCase(memberName.name);
            } else {
                rc = FunUtil.compareSiblingMembersByName(
                    rootMember,
                    searchMember);
            }
            if (rc == 0) {
                return rootMember;
            }
            if (!hierarchy.hasAll()) {
                if (matchType == MatchType.BEFORE) {
                    if (rc < 0
                        && (bestMatch == -1
                            || FunUtil.compareSiblingMembersByName(
                                rootMember,
                                rootMembers.get(bestMatch)) > 0))
                    {
                        bestMatch = k;
                    }
                } else if (matchType == MatchType.AFTER) {
                    if (rc > 0
                        && (bestMatch == -1
                            || FunUtil.compareSiblingMembersByName(
                                rootMember,
                                rootMembers.get(bestMatch)) < 0))
                    {
                        bestMatch = k;
                    }
                }
            }
        }

        if (matchType == MatchType.EXACT_SCHEMA) {
            return null;
        }

        if (matchType != MatchType.EXACT && bestMatch != -1) {
            return rootMembers.get(bestMatch);
        }
        // If the first level is 'all', lookup member at second level. For
        // example, they could say '[USA]' instead of '[(All
        // Customers)].[USA]'.
        if (rootMembers.size() <= 0 || !rootMembers.get(0).isAll()) {
            return null;
        }

        // REVIEW: Since we look up root members frequently -- whenever we
        // resolve an MDX name -- it would probably be better to retrieve
        // all root members, not just the one by a particular name.
        return reader.lookupMemberChildByName(
            rootMembers.get(0),
            memberName,
            matchType);
    }

    /**
     * Finds a named level in this hierarchy. Returns null if there is no
     * such level.
     */
    public static Level lookupHierarchyLevel(Hierarchy hierarchy, String s) {
        for (Level level : hierarchy.getLevelList()) {
            if (level.getName().equalsIgnoreCase(s)) {
                return level;
            }
        }
        return null;
    }

    /**
     * Finds the zero based ordinal of a Member among its siblings.
     */
    public static int getMemberOrdinalInParent(
        SchemaReader reader,
        Member member)
    {
        Member parent = member.getParentMember();
        List<Member> siblings =
            (parent == null)
            ? reader.getHierarchyRootMembers(member.getHierarchy())
            : reader.getMemberChildren(parent);

        for (int i = 0; i < siblings.size(); i++) {
            if (siblings.get(i).equals(member)) {
                return i;
            }
        }
        throw Util.newInternal(
            "could not find member " + member + " amongst its siblings");
    }

    /**
     * returns the first descendant on the level underneath parent.
     * If parent = [Time].[1997] and level = [Time].[Month], then
     * the member [Time].[1997].[Q1].[1] will be returned
     */
    public static Member getFirstDescendantOnLevel(
        SchemaReader reader,
        Member parent,
        Level level)
    {
        Member m = parent;
        while (m.getLevel() != level) {
            List<Member> children = reader.getMemberChildren(m);
            m = children.get(0);
        }
        return m;
    }

    /**
     * Returns whether a string is null or empty.
     */
    public static boolean isEmpty(String s) {
        return (s == null) || (s.length() == 0);
    }

    /**
     * Encloses a value in single-quotes, to make a SQL string value. Examples:
     * <code>singleQuoteForSql(null)</code> yields <code>NULL</code>;
     * <code>singleQuoteForSql("don't")</code> yields <code>'don''t'</code>.
     */
    public static String singleQuoteString(String val) {
        StringBuilder buf = new StringBuilder(64);
        singleQuoteString(val, buf);
        return buf.toString();
    }

    /**
     * Encloses a value in single-quotes, to make a SQL string value. Examples:
     * <code>singleQuoteForSql(null)</code> yields <code>NULL</code>;
     * <code>singleQuoteForSql("don't")</code> yields <code>'don''t'</code>.
     */
    public static void singleQuoteString(String val, StringBuilder buf) {
        buf.append('\'');

        String s0 = replace(val, "'", "''");
        buf.append(s0);

        buf.append('\'');
    }

    /**
     * Creates a random number generator.
     *
     * @param seed Seed for random number generator.
     *   If 0, generate a seed from the system clock and print the value
     *   chosen. (This is effectively non-deterministic.)
     *   If -1, generate a seed from an internal random number generator.
     *   (This is deterministic, but ensures that different tests have
     *   different seeds.)
     *
     * @return A random number generator.
     */
    public static Random createRandom(long seed) {
        if (seed == 0) {
            seed = new Random().nextLong();
            System.out.println("random: seed=" + seed);
        } else if (seed == -1 && metaRandom != null) {
            seed = metaRandom.nextLong();
        }
        return new Random(seed);
    }

    /**
     * Returns whether a property is valid for a member of a given level.
     * It is valid if the property is defined at the level or at
     * an ancestor level, or if the property is a standard property such as
     * "FORMATTED_VALUE".
     *
     * @param propertyName Property name
     * @param level Level
     * @return Whether property is valid
     */
    public static boolean isValidProperty(
        String propertyName,
        Level level)
    {
        return lookupProperty(level, propertyName) != null;
    }

    /**
     * Finds a member property called <code>propertyName</code> at, or above,
     * <code>level</code>.
     */
    public static Property lookupProperty(
        Level level,
        String propertyName)
    {
        boolean caseSensitive =
            MondrianProperties.instance().CaseSensitive.get();
        do {
            Property[] properties = level.getProperties();
            for (Property property : properties) {
                if (equal(property.getName(), propertyName, caseSensitive)) {
                    return property;
                }
            }
            level = level.getParentLevel();
        } while (level != null);
        // Now try a standard property.
        final Property property = Property.lookup(propertyName, caseSensitive);
        if (property != null
            && property.isMemberProperty()
            && property.isStandard())
        {
            return property;
        }
        return null;
    }

    /**
     * Insert a call to this method if you want to flag a piece of
     * undesirable code.
     *
     * @deprecated
     */
    public static <T> T deprecated(T reason) {
        throw new UnsupportedOperationException(reason.toString());
    }

    /**
     * Insert a call to this method if you want to flag a piece of
     * undesirable code.
     *
     * @deprecated
     */
    public static <T> T deprecated(T reason, boolean fail) {
        if (fail) {
            throw new UnsupportedOperationException(String.valueOf(reason));
        } else {
            return reason;
        }
    }

    /** Call this method to hit a break point. Remove calls to this method
     * before checking in code. */
    @Deprecated
    public static <T> T pauseIf(boolean condition, T... args) {
        if (condition) {
            Util.discard(0); // put a breakpoint here
        }
        return args.length > 0 ? args[0] : null;
    }

    public static List<Member> addLevelCalculatedMembers(
        SchemaReader reader,
        Level level,
        List<Member> members)
    {
        List<Member> calcMembers =
            reader.getCalculatedMembers(level);
        return Composite.optimalOf(members, calcMembers);
    }

    /**
     * Returns an exception which indicates that a particular piece of
     * functionality should work, but a developer has not implemented it yet.
     */
    public static RuntimeException needToImplement(Object o) {
        throw new UnsupportedOperationException("need to implement " + o);
    }

    /**
     * Returns an exception indicating that we didn't expect to find this value
     * here.
     */
    public static <T extends Enum<T>> RuntimeException badValue(
        Enum<T> anEnum)
    {
        return Util.newInternal(
            "Was not expecting value '" + anEnum
            + "' for enumeration '" + anEnum.getDeclaringClass().getName()
            + "' in this context");
    }

    /**
     * Converts a list of SQL-style patterns into a Java regular expression.
     *
     * <p>For example, {"Foo_", "Bar%BAZ"} becomes "Foo.|Bar.*BAZ".
     *
     * @param wildcards List of SQL-style wildcard expressions
     * @return Regular expression
     */
    public static String wildcardToRegexp(List<String> wildcards) {
        StringBuilder buf = new StringBuilder();
        for (String value : wildcards) {
            if (buf.length() > 0) {
                buf.append('|');
            }
            int i = 0;
            while (true) {
                int percent = value.indexOf('%', i);
                int underscore = value.indexOf('_', i);
                if (percent == -1 && underscore == -1) {
                    if (i < value.length()) {
                        buf.append(quotePattern(value.substring(i)));
                    }
                    break;
                }
                if (underscore >= 0 && (underscore < percent || percent < 0)) {
                    if (i < underscore) {
                        buf.append(
                            quotePattern(value.substring(i, underscore)));
                    }
                    buf.append('.');
                    i = underscore + 1;
                } else if (percent >= 0
                    && (percent < underscore || underscore < 0))
                {
                    if (i < percent) {
                    buf.append(
                        quotePattern(value.substring(i, percent)));
                    }
                    buf.append(".*");
                    i = percent + 1;
                } else {
                    throw new IllegalArgumentException();
                }
            }
        }
        return buf.toString();
    }

    /**
     * Converts a camel-case name to an upper-case name with underscores.
     *
     * <p>For example, <code>camelToUpper("FooBar")</code> returns "FOO_BAR".
     *
     * @param s Camel-case string
     * @return  Upper-case string
     */
    public static String camelToUpper(String s) {
        StringBuilder buf = new StringBuilder(s.length() + 10);
        int prevUpper = -1;
        for (int i = 0; i < s.length(); ++i) {
            char c = s.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > prevUpper + 1) {
                    buf.append('_');
                }
                prevUpper = i;
            } else {
                c = Character.toUpperCase(c);
            }
            buf.append(c);
        }
        return buf.toString();
    }

    /**
     * Parses a comma-separated list.
     *
     * <p>If a value contains a comma, escape it with a second comma. For
     * example, <code>parseCommaList("x,y,,z")</code> returns
     * <code>{"x", "y,z"}</code>.
     *
     * @param nameCommaList List of names separated by commas
     * @return List of names
     */
    public static List<String> parseCommaList(String nameCommaList) {
        if (nameCommaList.equals("")) {
            return Collections.emptyList();
        }
        if (nameCommaList.endsWith(",")) {
            // Special treatment for list ending in ",", because split ignores
            // entries after separator.
            final String zzz = "zzz";
            final List<String> list = parseCommaList(nameCommaList + zzz);
            String last = list.get(list.size() - 1);
            if (last.equals(zzz)) {
                list.remove(list.size() - 1);
            } else {
                list.set(
                    list.size() - 1,
                    last.substring(0, last.length() - zzz.length()));
            }
            return list;
        }
        List<String> names = new ArrayList<String>();
        final String[] strings = nameCommaList.split(",");
        for (String string : strings) {
            final int count = names.size();
            if (count > 0
                && names.get(count - 1).equals(""))
            {
                if (count == 1) {
                    if (string.equals("")) {
                        names.add("");
                    } else {
                        names.set(
                            0,
                            "," + string);
                    }
                } else {
                    names.set(
                        count - 2,
                        names.get(count - 2) + "," + string);
                    names.remove(count - 1);
                }
            } else {
                names.add(string);
            }
        }
        return names;
    }

    /**
     * Returns an annotation of a particular class on a method. Returns the
     * default value if the annotation is not present, or in JDK 1.4.
     *
     * @param method Method containing annotation
     * @param annotationClassName Name of annotation class to find
     * @param defaultValue Value to return if annotation is not present
     * @return value of annotation
     */
    public static <T> T getAnnotation(
        Method method,
        String annotationClassName,
        T defaultValue)
    {
        return compatible.getAnnotation(
            method, annotationClassName, defaultValue);
    }

    /**
     * Closes and cancels a {@link Statement} using the correct methods
     * available on the current Java runtime.
     * <p>If errors are encountered while canceling a statement,
     * the message is logged in {@link Util}.
     * @param stmt The statement to cancel.
     */
    public static void cancelStatement(Statement stmt) {
        compatible.cancelStatement(stmt);
    }

    public static MemoryInfo getMemoryInfo() {
        return compatible.getMemoryInfo();
    }

    /**
     * Converts a list of a string.
     *
     * For example,
     * <code>commaList("foo", Arrays.asList({"a", "b"}))</code>
     * returns "foo(a, b)".
     *
     * @param s Prefix
     * @param list List
     * @return String representation of string
     */
    public static <T> String commaList(
        String s,
        List<T> list)
    {
        final StringBuilder buf = new StringBuilder(s);
        buf.append("(");
        int k = -1;
        for (T t : list) {
            if (++k > 0) {
                buf.append(", ");
            }
            buf.append(t);
        }
        buf.append(")");
        return buf.toString();
    }

    /**
     * Converts a list of a string, with commas between elements.
     *
     * For example,
     * <code>commaList(Arrays.asList({"a", "b"}))</code>
     * returns "a, b".
     *
     * @param list List
     * @return String representation of string
     */
    public static <T> String commaList(List<T> list)
    {
        if (list.size() == 1) {
            return list.get(0).toString();
        }
        final StringBuilder buf = new StringBuilder();
        int k = -1;
        for (T t : list) {
            if (++k > 0) {
                buf.append(", ");
            }
            buf.append(t);
        }
        return buf.toString();
    }

    /**
     * Makes a name distinct from other names which have already been used
     * and shorter than a length limit, adds it to the list, and returns it.
     *
     * @param name Suggested name, may not be unique
     * @param maxLength Maximum length of generated name
     * @param nameList Collection of names already used
     *
     * @return Unique name
     */
    public static String uniquify(
        String name,
        int maxLength,
        Collection<String> nameList)
    {
        assert name != null;
        if (name.length() > maxLength) {
            name = name.substring(0, maxLength);
        }
        if (nameList.contains(name)) {
            String aliasBase = name;
            int j = 0;
            while (true) {
                name = aliasBase + j;
                if (name.length() > maxLength) {
                    aliasBase = aliasBase.substring(0, aliasBase.length() - 1);
                    continue;
                }
                if (!nameList.contains(name)) {
                    break;
                }
                j++;
            }
        }
        nameList.add(name);
        return name;
    }

    /**
     * Returns whether a collection contains precisely one distinct element.
     * Returns false if the collection is empty, or if it contains elements
     * that are not the same as each other.
     *
     * @param collection Collection
     * @return boolean true if all values are same
     */
    public static <T> boolean areOccurencesEqual(
        Collection<T> collection)
    {
        Iterator<T> it = collection.iterator();
        if (!it.hasNext()) {
            // Collection is empty
            return false;
        }
        T first = it.next();
        while (it.hasNext()) {
            T t = it.next();
            if (!t.equals(first)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether all of the elements in a collection are different.
     *
     * <p>Trivially true if the collection is a {@link Set}; if another kind
     * of collection, such as a {@link List}, internally forms a set to
     * detect duplicates.
     *
     * @param collection Collection
     * @return true if contents are distinct; false if same element occurs
     *    more than once
     */
    public static <T> boolean isDistinct(Collection<T> collection) {
        return collection instanceof Set
            || collection.size() < 2
            || new HashSet<T>(collection).size() == collection.size();
    }

    /**
     * Creates a memory-, CPU- and cache-efficient immutable list.
     *
     * @param t Array of members of list
     * @param <T> Element type
     * @return List containing the given members
     */
    public static <T> List<T> flatList(T... t) {
        return _flatList(t, false);
    }

    /**
     * Creates a memory-, CPU- and cache-efficient immutable list,
     * always copying the contents.
     *
     * @param t Array of members of list
     * @param <T> Element type
     * @return List containing the given members
     */
    public static <T> List<T> flatListCopy(T... t) {
        return _flatList(t, true);
    }

    /**
     * Creates a memory-, CPU- and cache-efficient immutable list, optionally
     * copying the list.
     *
     * @param copy Whether to always copy the list
     * @param t Array of members of list
     * @return List containing the given members
     */
    private static <T> List<T> _flatList(T[] t, boolean copy) {
        switch (t.length) {
        case 0:
            return COMPARABLE_EMPTY_LIST;
        case 1:
            return Collections.singletonList(t[0]);
        case 2:
            return new Flat2List<T>(t[0], t[1]);
        case 3:
            return new Flat3List<T>(t[0], t[1], t[2]);
        default:
            // REVIEW: AbstractList contains a modCount field; we could
            //   write our own implementation and reduce creation overhead a
            //   bit.
            if (copy) {
                return new ComparableList(Arrays.asList(t.clone()));
            } else {
                return new ComparableList(Arrays.asList(t));
            }
        }
    }

    /**
     * Creates a memory-, CPU- and cache-efficient immutable list from an
     * existing list. The list is always copied.
     *
     * @param t Array of members of list
     * @param <T> Element type
     * @return List containing the given members
     */
    public static <T> List<T> flatList(List<T> t) {
        switch (t.size()) {
        case 0:
            return COMPARABLE_EMPTY_LIST;
        case 1:
            return Collections.singletonList(t.get(0));
        case 2:
            return new Flat2List<T>(t.get(0), t.get(1));
        case 3:
            return new Flat3List<T>(t.get(0), t.get(1), t.get(2));
        default:
            // REVIEW: AbstractList contains a modCount field; we could
            //   write our own implementation and reduce creation overhead a
            //   bit.
            //noinspection unchecked
            return new ComparableList(Arrays.asList(t.toArray()));
        }
    }

    /**
     * Parses a locale string.
     *
     * <p>The inverse operation of {@link java.util.Locale#toString()}.
     *
     * @param localeString Locale string, e.g. "en" or "en_US"
     * @return Java locale object
     */
    public static Locale parseLocale(String localeString) {
        if (localeString.contains("-")) {
            // JDK 1.7 and later can parse BCP47 locales, e.g. "en-US".
            Locale locale = compatible.localeForLanguageTag(localeString);
            if (locale != null) {
                return locale;
            }
        }
        String[] strings = localeString.split("_");
        switch (strings.length) {
        case 1:
            return new Locale(strings[0]);
        case 2:
            return new Locale(strings[0], strings[1]);
        case 3:
            return new Locale(strings[0], strings[1], strings[2]);
        default:
            throw newInternal(
                "bad locale string '" + localeString + "'");
        }
    }

    private static final Map<String, String> TIME_UNITS =
        Olap4jUtil.mapOf(
            "ns", "NANOSECONDS",
            "us", "MICROSECONDS",
            "ms", "MILLISECONDS",
            "s", "SECONDS",
            "m", "MINUTES",
            "h", "HOURS",
            "d", "DAYS");

    /**
     * Parses an interval.
     *
     * <p>For example, "30s" becomes (30, {@link TimeUnit#SECONDS});
     * "2us" becomes (2, {@link TimeUnit#MICROSECONDS}).</p>
     *
     * <p>Units m (minutes), h (hours) and d (days) are only available
     * in JDK 1.6 or later, because the corresponding constants are missing
     * from {@link TimeUnit} in JDK 1.5.</p>
     *
     * @param s String to parse
     * @param unit Default time unit; may be null
     *
     * @return Pair of value and time unit. Neither pair or its components are
     * null
     *
     * @throws NumberFormatException if unit is not present and there is no
     * default, or if number is not valid
     */
    public static Pair<Long, TimeUnit> parseInterval(
        String s,
        TimeUnit unit)
        throws NumberFormatException
    {
        final String original = s;
        for (Map.Entry<String, String> entry : TIME_UNITS.entrySet()) {
            final String abbrev = entry.getKey();
            if (s.endsWith(abbrev)) {
                final String full = entry.getValue();
                try {
                    unit = TimeUnit.valueOf(full);
                    s = s.substring(0, s.length() - abbrev.length());
                    break;
                } catch (IllegalArgumentException e) {
                    // ignore - MINUTES, HOURS, DAYS are not defined in JDK1.5
                }
            }
        }
        if (unit == null) {
            throw new NumberFormatException(
                "Invalid time interval '" + original + "'. Does not contain a "
                + "time unit. (Suffix may be ns (nanoseconds), "
                + "us (microseconds), ms (milliseconds), s (seconds), "
                + "h (hours), d (days). For example, '20s' means 20 seconds.)");
        }
        try {
            return Pair.of(new BigDecimal(s).longValue(), unit);
        } catch (NumberFormatException e) {
            throw new NumberFormatException(
                "Invalid time interval '" + original + "'");
        }
    }

    /**
     * Converts a list of olap4j-style segments to a list of mondrian-style
     * segments.
     *
     * @param olap4jSegmentList List of olap4j segments
     * @return List of mondrian segments
     */
    public static List<Id.Segment> convert(
        List<IdentifierSegment> olap4jSegmentList)
    {
        final List<Id.Segment> list = new ArrayList<Id.Segment>();
        for (IdentifierSegment olap4jSegment : olap4jSegmentList) {
            list.add(convert(olap4jSegment));
        }
        return list;
    }

    /**
     * Converts an olap4j-style segment to a mondrian-style segment.
     *
     * @param olap4jSegment olap4j segment
     * @return mondrian segment
     */
    public static Id.Segment convert(IdentifierSegment olap4jSegment) {
        if (olap4jSegment instanceof NameSegment) {
            return convert((NameSegment) olap4jSegment);
        } else {
            return convert((KeySegment) olap4jSegment);
        }
    }

    private static Id.KeySegment convert(final KeySegment keySegment) {
        return new Id.KeySegment(
            new AbstractList<Id.NameSegment>() {
                public Id.NameSegment get(int index) {
                    return convert(keySegment.getKeyParts().get(index));
                }

                public int size() {
                    return keySegment.getKeyParts().size();
                }
            });
    }

    private static Id.NameSegment convert(NameSegment nameSegment) {
        return new Id.NameSegment(
            nameSegment.getName(),
            convert(nameSegment.getQuoting()));
    }

    private static Id.Quoting convert(Quoting quoting) {
        switch (quoting) {
        case QUOTED:
            return Id.Quoting.QUOTED;
        case UNQUOTED:
            return Id.Quoting.UNQUOTED;
        case KEY:
            return Id.Quoting.KEY;
        default:
            throw Util.unexpected(quoting);
        }
    }

    /**
     * Applies a collection of filters to an iterable.
     *
     * @param iterable Iterable
     * @param conds Zero or more conditions
     * @param <T>
     * @return Iterable that returns only members of underlying iterable for
     *     for which all conditions evaluate to true
     */
    public static <T> Iterable<T> filter(
        final Iterable<T> iterable,
        final Predicate1<T>... conds)
    {
        final Predicate1<T>[] conds2 = optimizeConditions(conds);
        if (conds2.length == 0) {
            return iterable;
        }
        return new Iterable<T>() {
            public Iterator<T> iterator() {
                return new Iterator<T>() {
                    final Iterator<T> iterator = iterable.iterator();
                    T next;
                    boolean hasNext = moveToNext();

                    private boolean moveToNext() {
                        outer:
                        while (iterator.hasNext()) {
                            next = iterator.next();
                            for (Predicate1<T> cond : conds2) {
                                if (!cond.test(next)) {
                                    continue outer;
                                }
                            }
                            return true;
                        }
                        return false;
                    }

                    public boolean hasNext() {
                        return hasNext;
                    }

                    public T next() {
                        T t = next;
                        hasNext = moveToNext();
                        return t;
                    }

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    private static <T> Predicate1<T>[] optimizeConditions(
        Predicate1<T>[] conds)
    {
        final List<Predicate1<T>> predicateList =
            new ArrayList<Predicate1<T>>(Arrays.asList(conds));
        for (Iterator<Predicate1<T>> funcIter = predicateList.iterator();
            funcIter.hasNext();)
        {
            Predicate1<T> predicate = funcIter.next();
            if (predicate == truePredicate1()) {
                funcIter.remove();
            }
        }
        if (predicateList.size() < conds.length) {
            //noinspection unchecked
            return predicateList.toArray(new Predicate1[predicateList.size()]);
        } else {
            return conds;
        }
    }

    /**
     * Sorts a collection of {@link Comparable} objects and returns a list.
     *
     * @param collection Collection
     * @param <T> Element type
     * @return Sorted list
     */
    public static <T extends Comparable> List<T> sort(
        Collection<T> collection)
    {
        Object[] a = collection.toArray(new Object[collection.size()]);
        Arrays.sort(a);
        return cast(Arrays.asList(a));
    }

    /**
     * Sorts a collection of objects using a {@link java.util.Comparator} and
     * returns a list.
     *
     * @param collection Collection
     * @param comparator Comparator
     * @param <T> Element type
     * @return Sorted list
     */
    public static <T> List<T> sort(
        Collection<T> collection,
        Comparator<T> comparator)
    {
        Object[] a = collection.toArray(new Object[collection.size()]);
        //noinspection unchecked
        Arrays.sort(a, (Comparator<? super Object>) comparator);
        return cast(Arrays.asList(a));
    }

    public static List<IdentifierSegment> toOlap4j(
        final List<Id.Segment> segments)
    {
        return new AbstractList<IdentifierSegment>() {
            public IdentifierSegment get(int index) {
                return toOlap4j(segments.get(index));
            }

            public int size() {
                return segments.size();
            }
        };
    }

    public static IdentifierSegment toOlap4j(Id.Segment segment) {
        switch (segment.quoting) {
        case KEY:
            return toOlap4j((Id.KeySegment) segment);
        default:
            return toOlap4j((Id.NameSegment) segment);
        }
    }

    private static KeySegment toOlap4j(final Id.KeySegment keySegment) {
        return new KeySegment(
            new AbstractList<NameSegment>() {
                public NameSegment get(int index) {
                    return toOlap4j(keySegment.subSegmentList.get(index));
                }

                public int size() {
                    return keySegment.subSegmentList.size();
                }
            });
    }

    private static NameSegment toOlap4j(Id.NameSegment nameSegment) {
        return new NameSegment(
            null,
            nameSegment.name,
            toOlap4j(nameSegment.quoting));
    }

    public static Quoting toOlap4j(Id.Quoting quoting) {
        return Quoting.valueOf(quoting.name());
    }

    // TODO: move to IdentifierSegment
    public static boolean matches(IdentifierSegment segment, String name) {
        switch (segment.getQuoting()) {
        case KEY:
            return false; // FIXME
        case QUOTED:
            return equalName(segment.getName(), name);
        case UNQUOTED:
            return segment.getName().equalsIgnoreCase(name);
        default:
            throw unexpected(segment.getQuoting());
        }
    }

    public static RuntimeException newElementNotFoundException(
        int category,
        IdentifierNode identifierNode)
    {
        String type;
        switch (category) {
        case Category.Member:
            return MondrianResource.instance().MemberNotFound.ex(
                identifierNode.toString());
        case Category.Unknown:
            type = "Element";
            break;
        default:
            type = Category.instance().getDescription(category);
        }
        return newError(type + " '" + identifierNode + "' not found");
    }

    /**
     * Calls {@link java.util.concurrent.Future#get()} and converts any
     * throwable into a non-checked exception.
     *
     * @param future Future
     * @param message Message to qualify wrapped exception
     * @param <T> Result type
     * @return Result
     */
    public static <T> T safeGet(Future<T> future, String message) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw newError(e, message);
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            } else if (cause instanceof Error) {
                throw (Error) cause;
            } else {
                throw newError(cause, message);
            }
        }
    }

    public static <T> Set<T> newIdentityHashSetFake() {
        final HashMap<T, Boolean> map = new HashMap<T, Boolean>();
        return new Set<T>() {
            public int size() {
                return map.size();
            }

            public boolean isEmpty() {
                return map.isEmpty();
            }

            public boolean contains(Object o) {
                return map.containsKey(o);
            }

            public Iterator<T> iterator() {
                return map.keySet().iterator();
            }

            public Object[] toArray() {
                return map.keySet().toArray();
            }

            public <T> T[] toArray(T[] a) {
                return map.keySet().toArray(a);
            }

            public boolean add(T t) {
                return map.put(t, Boolean.TRUE) == null;
            }

            public boolean remove(Object o) {
                return map.remove(o) == Boolean.TRUE;
            }

            public boolean containsAll(Collection<?> c) {
                return map.keySet().containsAll(c);
            }

            public boolean addAll(Collection<? extends T> c) {
                throw new UnsupportedOperationException();
            }

            public boolean retainAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            public boolean removeAll(Collection<?> c) {
                throw new UnsupportedOperationException();
            }

            public void clear() {
                map.clear();
            }
        };
    }

    /**
     * Equivalent to {@link Timer#Timer(String, boolean)}.
     * (Introduced in JDK 1.5.)
     *
     * @param name the name of the associated thread
     * @param isDaemon true if the associated thread should run as a daemon
     * @return timer
     */
    public static Timer newTimer(String name, boolean isDaemon) {
        return compatible.newTimer(name, isDaemon);
    }

    /**
     * As Arrays#binarySearch(Object[], int, int, Object), but
     * available pre-JDK 1.6.
     */
    public static <T extends Comparable<T>> int binarySearch(
        T[] ts, int start, int end, T t)
    {
        return compatible.binarySearch(ts, start, end, t);
    }

    /**
     * Returns the intersection of two sorted sets. Does not modify either set.
     *
     * <p>Optimized for the case that both sets are {@link ArraySortedSet}.</p>
     *
     * @param set1 First set
     * @param set2 Second set
     * @return Intersection of the sets
     */
    public static <E extends Comparable> SortedSet<E> intersect(
        SortedSet<E> set1,
        SortedSet<E> set2)
    {
        if (set1.isEmpty()) {
            return set1;
        }
        if (set2.isEmpty()) {
            return set2;
        }
        if (!(set1 instanceof ArraySortedSet)
            || !(set2 instanceof ArraySortedSet))
        {
            final TreeSet<E> set = new TreeSet<E>(set1);
            set.retainAll(set2);
            return set;
        }
        final Comparable<?>[] result =
            new Comparable[Math.min(set1.size(), set2.size())];
        final Iterator<E> it1 = set1.iterator();
        final Iterator<E> it2 = set2.iterator();
        int i = 0;
        E e1 = it1.next();
        E e2 = it2.next();
        for (;;) {
            final int compare = e1.compareTo(e2);
            if (compare == 0) {
                result[i++] = e1;
                if (!it1.hasNext() || !it2.hasNext()) {
                    break;
                }
                e1 = it1.next();
                e2 = it2.next();
            } else if (compare == 1) {
                if (!it2.hasNext()) {
                    break;
                }
                e2 = it2.next();
            } else {
                if (!it1.hasNext()) {
                    break;
                }
                e1 = it1.next();
            }
        }
        return new ArraySortedSet(result, 0, i);
    }

    /**
     * Returns the last item in a list.
     *
     * @param list List
     * @param <T> Element type
     * @return Last item in the list
     * @throws IndexOutOfBoundsException if list is empty
     */
    public static <T> T last(List<T> list) {
        return list.get(list.size() - 1);
    }

    /**
     * Returns the sole item in a list.
     *
     * <p>If the list has 0 or more than one element, throws.</p>
     *
     * @param list List
     * @param <T> Element type
     * @return Sole item in the list
     * @throws IndexOutOfBoundsException if list is empty or has more than 1 elt
     */
    public static <T> T only(List<T> list) {
        if (list.size() != 1) {
            throw new IndexOutOfBoundsException(
                "list " + list + " has " + list.size()
                + " elements, expected 1");
        }
        return list.get(0);
    }

    /**
     * Sets or clears a given bit in an integer.
     *
     * @param i Initial integer
     * @param bit Bit to set
     * @param set Whether to set or clear
     * @return Integer with bit set or cleared
     */
    public static int bit(int i, int bit, boolean set) {
        return i & ~(set ? 0 : (1 << bit)) | (set ? (1 << bit) : 0);
    }

    /**
     * Returns the first argument that is not null.
     *
     * <p>You can use this method to provide defaults, e.g.
     * {@code first(foo.name, "anonymous")}.</p>
     *
     * @param s0 Argument one
     * @param s1 Argument two
     * @param <T> Type of arguments and result
     * @return First argument that is not null.
     */
    public static <T> T first(T s0, T s1) {
        if (s0 != null) {
            return s0;
        }
        return s1;
    }

    /**
     * Computes <a href="http://en.wikipedia.org/wiki/Julian_day">Julian Day
     * Number</a>.</p>
     *
     * @param year Year
     * @param month Month
     * @param day Date
     * @return Julian Day Number (JDN)
     */
    public static long julian(long year, long month, long day) {
        long a = (14 - month) / 12;
        long y = year + 4800 - a;
        long m = month + 12 * a - 3;
        return day
            + (153 * m + 2) / 5
            + 365 * y
            + y / 4
            - y / 100
            + y / 400
            - 32045;
    }

    public static <T> List<T> toList(Iterable<? extends T> iterable) {
        final List<T> list = new ArrayList<T>();
        for (T t : iterable) {
            list.add(t);
        }
        return list;
    }

    /**
     * Closes a JDBC result set, statement, and connection, ignoring any errors.
     * If any of them are null, that's fine.
     *
     * <p>If any of them throws a {@link SQLException}, returns the first
     * such exception, but always executes all closes.</p>
     *
     * @param resultSet Result set
     * @param statement Statement
     * @param connection Connection
     */
    public static SQLException close(
        ResultSet resultSet,
        Statement statement,
        Connection connection)
    {
        SQLException firstException = null;
        if (resultSet != null) {
            try {
                if (statement == null) {
                    statement = resultSet.getStatement();
                }
                resultSet.close();
            } catch (Throwable t) {
                firstException = new SQLException();
                firstException.initCause(t);
            }
        }
        if (statement != null) {
            try {
                statement.close();
            } catch (Throwable t) {
                if (firstException == null) {
                    firstException = new SQLException();
                    firstException.initCause(t);
                }
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Throwable t) {
                if (firstException == null) {
                    firstException = new SQLException();
                    firstException.initCause(t);
                }
            }
        }
        return firstException;
    }

    /**
     * Adds a (key, value) pair to a multi-map represented as a map of lists.
     * A "put" may add a new key, or it may add a new value to the list of an
     * existing key.
     *
     * @param map Map
     * @param k Key
     * @param v Value
     * @param <K> Key type
     * @param <V> Value type
     */
    public static <K, V> void putMulti(Map<K, List<V>> map, K k, V v) {
        List<V> list = map.put(k, Collections.singletonList(v));
        if (list != null) {
            if (list.size() == 1) {
                list = new ArrayList<V>(list);
            }
            list.add(v);
            map.put(k, list);
        }
    }

    /**
     * Adds a (key, bit) pair to a multi-map represented as a map of
     * {@link BitSet}s. A "put" may add a new bit set, or it may add a new value
     * to the list of an existing bit set.
     *
     * @param map Map
     * @param k Key
     * @param v Value
     * @param <K> Key type
     */
    public static <K> void putMulti(Map<K, BitSet> map, K k, int v) {
        BitSet bitSet = map.get(k);
        if (bitSet == null) {
            bitSet = new BitSet();
            map.put(k, bitSet);
        }
        bitSet.set(v);
    }

    /** Returns a subset of a list whose elements pass a given predicate. */
    public static <E> List<E> copyWhere(List<E> list, Predicate1<E> predicate) {
        final int size = list.size();
        List<E> result = new ArrayList<E>(size);
        for (E e : list) {
            if (predicate.apply(e)) {
                result.add(e);
            }
        }
        // Don't make a copy if all elements pass predicate.
        return result.size() == size ? list : result;
    }

    /** Returns a view of a list obtained by applying a function to each
     * element. */
    public static <P, R> List<R> transform(
        final Function1<P, R> fn,
        final List<P> list)
    {
        return new AbstractList<R>() {
            public R get(int index) {
                return fn.apply(list.get(index));
            }

            public int size() {
                return list.size();
            }
        };
    }

    /** Returns a list with all elements from {@code from} onwards. */
    public static <E> List<E> subList(List<E> list, int from) {
        return list.subList(from, list.size());
    }

    /**
     * Creates a bitset with bits from {@code fromIndex} (inclusive) to
     * specified {@code toIndex} (exclusive) set to {@code true}.
     *
     * <p>For example, {@code bitSetBetween(0, 3)} returns a bit set with bits
     * {0, 1, 2} set.
     *
     * @param fromIndex Index of the first bit to be set.
     * @param toIndex   Index after the last bit to be set.
     * @return Bit set
     */
    public static BitSet bitSetBetween(int fromIndex, int toIndex) {
        final BitSet bitSet = new BitSet();
        if (toIndex > fromIndex) {
            // Avoid http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6222207
            // "BitSet internal invariants may be violated"
            bitSet.set(fromIndex, toIndex);
        }
        return bitSet;
    }


    public static class ErrorCellValue {
        public String toString() {
            return "#ERR";
        }
    }

    @SuppressWarnings({"unchecked"})
    public static <T> T[] genericArray(Class<T> clazz, int size) {
        return (T[]) Array.newInstance(clazz, size);
    }

    /**
     * Throws an internal error if condition is not true. It would be called
     * <code>assert</code>, but that is a keyword as of JDK 1.4.
     */
    public static void assertTrue(boolean b) {
        if (!b) {
            throw newInternal("assert failed");
        }
    }

    /**
     * Throws an internal error with the given messagee if condition is not
     * true. It would be called <code>assert</code>, but that is a keyword as
     * of JDK 1.4.
     */
    public static void assertTrue(boolean b, String message) {
        if (!b) {
            throw newInternal("assert failed: " + message);
        }
    }

    /**
     * Creates an internal error with a given message.
     */
    public static RuntimeException newInternal(String message) {
        return MondrianResource.instance().Internal.ex(message);
    }

    /**
     * Creates an internal error with a given message and cause.
     */
    public static RuntimeException newInternal(Throwable e, String message) {
        return MondrianResource.instance().Internal.ex(message, e);
    }

    /**
     * Creates a non-internal error. Currently implemented in terms of
     * internal errors, but later we will create resourced messages.
     */
    public static RuntimeException newError(String message) {
        return newInternal(message);
    }

    /**
     * Creates a non-internal error. Currently implemented in terms of
     * internal errors, but later we will create resourced messages.
     */
    public static RuntimeException newError(Throwable e, String message) {
        return newInternal(e, message);
    }

    /**
     * Returns an exception indicating that we didn't expect to find this value
     * here.
     *
     * @param value Value
     */
    public static RuntimeException unexpected(Enum value) {
        return Util.newInternal(
            "Was not expecting value '" + value
            + "' for enumeration '" + value.getClass().getName()
            + "' in this context");
    }

    /**
     * Checks that a precondition (declared using the javadoc <code>@pre</code>
     * tag) is satisfied.
     *
     * @param b The value of executing the condition
     */
    public static void assertPrecondition(boolean b) {
        assertTrue(b);
    }

    /**
     * Checks that a precondition (declared using the javadoc <code>@pre</code>
     * tag) is satisfied. For example,
     *
     * <blockquote><pre>void f(String s) {
     *    Util.assertPrecondition(s != null, "s != null");
     *    ...
     * }</pre></blockquote>
     *
     * @param b The value of executing the condition
     * @param condition The text of the condition
     */
    public static void assertPrecondition(boolean b, String condition) {
        assertTrue(b, condition);
    }

    /**
     * Checks that a postcondition (declared using the javadoc
     * <code>@post</code> tag) is satisfied.
     *
     * @param b The value of executing the condition
     */
    public static void assertPostcondition(boolean b) {
        assertTrue(b);
    }

    /**
     * Checks that a postcondition (declared using the javadoc
     * <code>@post</code> tag) is satisfied.
     *
     * @param b The value of executing the condition
     */
    public static void assertPostcondition(boolean b, String condition) {
        assertTrue(b, condition);
    }

    /**
     * Converts an error into an array of strings, the most recent error first.
     *
     * @param e the error; may be null. Errors are chained according to their
     *    {@link Throwable#getCause cause}.
     */
    public static String[] convertStackToString(Throwable e) {
        List<String> list = new ArrayList<String>();
        while (e != null) {
            String sMsg = getErrorMessage(e);
            list.add(sMsg);
            e = e.getCause();
        }
        return list.toArray(new String[list.size()]);
    }

    /**
     * Constructs the message associated with an arbitrary Java error, making
     * up one based on the stack trace if there is none. As
     * {@link #getErrorMessage(Throwable,boolean)}, but does not print the
     * class name if the exception is derived from {@link java.sql.SQLException}
     * or is exactly a {@link java.lang.Exception}.
     */
    public static String getErrorMessage(Throwable err) {
        boolean prependClassName =
            !(err instanceof java.sql.SQLException
              || err.getClass() == java.lang.Exception.class);
        return getErrorMessage(err, prependClassName);
    }

    /**
     * Constructs the message associated with an arbitrary Java error, making
     * up one based on the stack trace if there is none.
     *
     * @param err the error
     * @param prependClassName should the error be preceded by the
     *   class name of the Java exception?  defaults to false, unless the error
     *   is derived from {@link java.sql.SQLException} or is exactly a {@link
     *   java.lang.Exception}
     */
    public static String getErrorMessage(
        Throwable err,
        boolean prependClassName)
    {
        String errMsg = err.getMessage();
        if ((errMsg == null) || (err instanceof RuntimeException)) {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            err.printStackTrace(pw);
            return sw.toString();
        } else {
            return (prependClassName)
                ? err.getClass().getName() + ": " + errMsg
                : errMsg;
        }
    }

    /**
     * If one of the causes of an exception is of a particular class, returns
     * that cause. Otherwise returns null.
     *
     * @param e Exception
     * @param clazz Desired class
     * @param <T> Class
     * @return Cause of given class, or null
     */
    public static <T extends Throwable>
    T getMatchingCause(Throwable e, Class<T> clazz) {
        for (;;) {
            if (clazz.isInstance(e)) {
                return clazz.cast(e);
            }
            final Throwable cause = e.getCause();
            if (cause == null || cause == e) {
                return null;
            }
            e = cause;
        }
    }

    /**
     * Converts an expression to a string.
     */
    public static String unparse(Exp exp) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        exp.unparse(pw);
        return sw.toString();
    }

    /**
     * Converts an query to a string.
     */
    public static String unparse(Query query) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new QueryPrintWriter(sw);
        query.unparse(pw);
        return sw.toString();
    }

    /**
     * Creates a file-protocol URL for the given file.
     */
    public static URL toURL(File file) throws MalformedURLException {
        String path = file.getAbsolutePath();
        // This is a bunch of weird code that is required to
        // make a valid URL on the Windows platform, due
        // to inconsistencies in what getAbsolutePath returns.
        String fs = System.getProperty("file.separator");
        if (fs.length() == 1) {
            char sep = fs.charAt(0);
            if (sep != '/') {
                path = path.replace(sep, '/');
            }
            if (path.charAt(0) != '/') {
                path = '/' + path;
            }
        }
        path = "file://" + path;
        return new URL(path);
    }

    /**
     * <code>PropertyList</code> is an order-preserving list of key-value
     * pairs. Lookup is case-insensitive, but the case of keys is preserved.
     */
    public static class PropertyList
        implements Iterable<Pair<String, String>>, Serializable
    {
        List<Pair<String, String>> list =
            new ArrayList<Pair<String, String>>();

        public PropertyList() {
            this.list = new ArrayList<Pair<String, String>>();
        }

        private PropertyList(List<Pair<String, String>> list) {
            this.list = list;
        }

        @SuppressWarnings({"CloneDoesntCallSuperClone"})
        @Override
        public PropertyList clone() {
            // Copy pairs, because pairs are modified when put is called.
            final List<Pair<String, String>> list1 =
                new ArrayList<Pair<String, String>>(list.size());
            for (Pair<String, String> pair : list) {
                list1.add(Pair.of(pair.left, pair.right));
            }
            return new PropertyList(list1);
        }

        public String get(String key) {
            return get(key, null);
        }

        public String get(String key, String defaultValue) {
            for (Pair<String, String> pair : list) {
                if (pair.left.equalsIgnoreCase(key)) {
                    return pair.right;
                }
            }
            return defaultValue;
        }

        public String put(String key, String value) {
            for (Pair<String, String> pair : list) {
                if (pair.left.equalsIgnoreCase(key)) {
                    String old = pair.right;
                    if (key.equalsIgnoreCase("Provider")) {
                        // Unlike all other properties, later values of
                        // "Provider" do not supersede
                    } else {
                        pair.right = value;
                    }
                    return old;
                }
            }
            list.add(Pair.of(key, value));
            return null;
        }

        public boolean remove(String key) {
            boolean found = false;
            for (int i = 0; i < list.size(); i++) {
                Pair<String, String> pair = list.get(i);
                if (pair.getKey().equalsIgnoreCase(key)) {
                    list.remove(i);
                    found = true;
                    --i;
                }
            }
            return found;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder(64);
            for (int i = 0, n = list.size(); i < n; i++) {
                Pair<String, String> pair = list.get(i);
                if (i > 0) {
                    sb.append("; ");
                }
                sb.append(pair.left);
                sb.append('=');

                final String right = pair.right;
                if (right == null) {
                    sb.append("'null'");
                } else {
                    // Quote a property value if is has a semi colon in it
                    // 'xxx;yyy'. Escape any single-quotes by doubling them.
                    final int needsQuote = right.indexOf(';');
                    if (needsQuote >= 0) {
                        // REVIEW: This logic leaves off the leading/trailing
                        //   quote if the property value already has a
                        //   leading/trailing quote. Doesn't seem right to me.
                        if (right.charAt(0) != '\'') {
                            sb.append("'");
                        }
                        sb.append(replace(right, "'", "''"));
                        if (right.charAt(right.length() - 1) != '\'') {
                            sb.append("'");
                        }
                    } else {
                        sb.append(right);
                    }
                }
            }
            return sb.toString();
        }

        public Iterator<Pair<String, String>> iterator() {
            return list.iterator();
        }
    }

    /**
     * Converts an OLE DB connect string into a {@link PropertyList}.
     *
     * <p> For example, <code>"Provider=MSOLAP; DataSource=LOCALHOST;"</code>
     * becomes the set of (key, value) pairs <code>{("Provider","MSOLAP"),
     * ("DataSource", "LOCALHOST")}</code>. Another example is
     * <code>Provider='sqloledb';Data Source='MySqlServer';Initial
     * Catalog='Pubs';Integrated Security='SSPI';</code>.
     *
     * <p> This method implements as much as possible of the <a
     * href="http://msdn.microsoft.com/library/en-us/oledb/htm/oledbconnectionstringsyntax.asp"
     * target="_blank">OLE DB connect string syntax
     * specification</a>. To find what it <em>actually</em> does, take
     * a look at the <code>mondrian.olap.UtilTestCase</code> test case.
     */
    public static PropertyList parseConnectString(String s) {
        return new ConnectStringParser(s).parse();
    }

    private static class ConnectStringParser {
        private final String s;
        private final int n;
        private int i;
        private final StringBuilder nameBuf;
        private final StringBuilder valueBuf;

        private ConnectStringParser(String s) {
            this.s = s;
            this.i = 0;
            this.n = s.length();
            this.nameBuf = new StringBuilder(64);
            this.valueBuf = new StringBuilder(64);
        }

        PropertyList parse() {
            PropertyList list = new PropertyList();
            while (i < n) {
                parsePair(list);
            }
            return list;
        }
        /**
         * Reads "name=value;" or "name=value<EOF>".
         */
        void parsePair(PropertyList list) {
            String name = parseName();
            if (name == null) {
                return;
            }
            String value;
            if (i >= n) {
                value = "";
            } else if (s.charAt(i) == ';') {
                i++;
                value = "";
            } else {
                value = parseValue();
            }
            list.put(name, value);
        }

        /**
         * Reads "name=". Name can contain equals sign if equals sign is
         * doubled. Returns null if there is no name to read.
         */
        String parseName() {
            nameBuf.setLength(0);
            while (true) {
                char c = s.charAt(i);
                switch (c) {
                case '=':
                    i++;
                    if (i < n && (c = s.charAt(i)) == '=') {
                        // doubled equals sign; take one of them, and carry on
                        i++;
                        nameBuf.append(c);
                        break;
                    }
                    String name = nameBuf.toString();
                    name = name.trim();
                    return name;
                case ' ':
                    if (nameBuf.length() == 0) {
                        // ignore preceding spaces
                        i++;
                        if (i >= n) {
                            // there is no name, e.g. trailing spaces after
                            // semicolon, 'x=1; y=2; '
                            return null;
                        }
                        break;
                    } else {
                        // fall through
                    }
                default:
                    nameBuf.append(c);
                    i++;
                    if (i >= n) {
                        return nameBuf.toString().trim();
                    }
                }
            }
        }

        /**
         * Reads "value;" or "value<EOF>"
         */
        String parseValue() {
            char c;
            // skip over leading white space
            while ((c = s.charAt(i)) == ' ') {
                i++;
                if (i >= n) {
                    return "";
                }
            }
            if (c == '"' || c == '\'') {
                String value = parseQuoted(c);
                // skip over trailing white space
                while (i < n && (c = s.charAt(i)) == ' ') {
                    i++;
                }
                if (i >= n) {
                    return value;
                } else if (s.charAt(i) == ';') {
                    i++;
                    return value;
                } else {
                    throw new RuntimeException(
                        "quoted value ended too soon, at position " + i
                        + " in '" + s + "'");
                }
            } else {
                String value;
                int semi = s.indexOf(';', i);
                if (semi >= 0) {
                    value = s.substring(i, semi);
                    i = semi + 1;
                } else {
                    value = s.substring(i);
                    i = n;
                }
                return value.trim();
            }
        }
        /**
         * Reads a string quoted by a given character. Occurrences of the
         * quoting character must be doubled. For example,
         * <code>parseQuoted('"')</code> reads <code>"a ""new"" string"</code>
         * and returns <code>a "new" string</code>.
         */
        String parseQuoted(char q) {
            char c = s.charAt(i++);
            Util.assertTrue(c == q);
            valueBuf.setLength(0);
            while (i < n) {
                c = s.charAt(i);
                if (c == q) {
                    i++;
                    if (i < n) {
                        c = s.charAt(i);
                        if (c == q) {
                            valueBuf.append(c);
                            i++;
                            continue;
                        }
                    }
                    return valueBuf.toString();
                } else {
                    valueBuf.append(c);
                    i++;
                }
            }
            throw new RuntimeException(
                "Connect string '" + s
                + "' contains unterminated quoted value '" + valueBuf.toString()
                + "'");
        }
    }

    /**
     * Combines two integers into a hash code.
     */
    public static int hash(int i, int j) {
        return (i << 4) ^ j;
    }

    /**
     * Computes a hash code from an existing hash code and an object (which
     * may be null).
     */
    public static int hash(int h, Object o) {
        int k = (o == null) ? 0 : o.hashCode();
        return ((h << 4) | h) ^ k;
    }

    /**
     * Computes a hash code from an existing hash code and one or more objects
     * (any of which may be null).
     *
     * @param h Existing hash code
     * @param a Array of zero or more arguments
     * @return Hash code of h and each object in array
     */
    public static int hashV(int h, Object... a) {
        return hashArray(h, a);
    }

    /**
     * Computes a hash code from an existing hash code and an array of objects
     * (which may be null).
     *
     * @param h Existing hash code
     * @param a Array of arguments
     * @return Hash code of h and each object in array
     */
    public static int hashArray(int h, Object[] a) {
        // The hashcode for a null array and an empty array should be different
        // than h, so use magic numbers.
        if (a == null) {
            return hash(h, 19690429);
        }
        if (a.length == 0) {
            return hash(h, 19690721);
        }
        for (Object anA : a) {
            h = hash(h, anA);
        }
        return h;
    }

    /**
     * Concatenates one or more arrays.
     *
     * <p>Resulting array has same element type as first array. Each arrays may
     * be empty, but must not be null.
     *
     * @param a0 First array
     * @param as Zero or more subsequent arrays
     * @return Array containing all elements
     */
    public static <T> T[] appendArrays(
        T[] a0,
        T[]... as)
    {
        int n = a0.length;
        for (T[] a : as) {
            n += a.length;
        }
        T[] copy = Util.copyOf(a0, n);
        n = a0.length;
        for (T[] a : as) {
            System.arraycopy(a, 0, copy, n, a.length);
            n += a.length;
        }
        return copy;
    }

    /**
     * Adds an object to the end of an array.  The resulting array is of the
     * same type (e.g. <code>String[]</code>) as the input array.
     *
     * @param a Array
     * @param o Element
     * @return New array containing original array plus element
     *
     * @see #appendArrays
     */
    public static <T> T[] append(T[] a, T o) {
        T[] a2 = Util.copyOf(a, a.length + 1);
        a2[a.length] = o;
        return a2;
    }

    /**
     * Like <code>{@link java.util.Arrays}.copyOf(double[], int)</code>, but
     * exists prior to JDK 1.6.
     *
     * @param original the array to be copied
     * @param newLength the length of the copy to be returned
     * @return a copy of the original array, truncated or padded with zeros
     *     to obtain the specified length
     */
    public static double[] copyOf(double[] original, int newLength) {
        double[] copy = new double[newLength];
        System.arraycopy(
            original, 0, copy, 0, Math.min(original.length, newLength));
        return copy;
    }

    /**
     * Like <code>{@link java.util.Arrays}.copyOf(int[], int)</code>, but
     * exists prior to JDK 1.6.
     *
     * @param original the array to be copied
     * @param newLength the length of the copy to be returned
     * @return a copy of the original array, truncated or padded with zeros
     *     to obtain the specified length
     */
    public static int[] copyOf(int[] original, int newLength) {
        int[] copy = new int[newLength];
        System.arraycopy(
            original, 0, copy, 0, Math.min(original.length, newLength));
        return copy;
    }

    /**
     * Like <code>{@link java.util.Arrays}.copyOf(long[], int)</code>, but
     * exists prior to JDK 1.6.
     *
     * @param original the array to be copied
     * @param newLength the length of the copy to be returned
     * @return a copy of the original array, truncated or padded with zeros
     *     to obtain the specified length
     */
    public static long[] copyOf(long[] original, int newLength) {
        long[] copy = new long[newLength];
        System.arraycopy(
            original, 0, copy, 0, Math.min(original.length, newLength));
        return copy;
    }

    /**
     * Like <code>{@link java.util.Arrays}.copyOf(Object[], int)</code>, but
     * exists prior to JDK 1.6.
     *
     * @param original the array to be copied
     * @param newLength the length of the copy to be returned
     * @return a copy of the original array, truncated or padded with zeros
     *     to obtain the specified length
     */
    public static <T> T[] copyOf(T[] original, int newLength) {
        //noinspection unchecked
        return (T[]) copyOf(original, newLength, original.getClass());
    }

    /**
     * Copies the specified array.
     *
     * @param original the array to be copied
     * @param newLength the length of the copy to be returned
     * @param newType the class of the copy to be returned
     * @return a copy of the original array, truncated or padded with nulls
     *     to obtain the specified length
     */
    public static <T, U> T[] copyOf(
        U[] original, int newLength, Class<? extends T[]> newType)
    {
        @SuppressWarnings({"unchecked", "RedundantCast"})
        T[] copy = ((Object)newType == (Object)Object[].class)
            ? (T[]) new Object[newLength]
            : (T[]) Array.newInstance(newType.getComponentType(), newLength);
        //noinspection SuspiciousSystemArraycopy
        System.arraycopy(
            original, 0, copy, 0,
            Math.min(original.length, newLength));
        return copy;
    }

    /**
     * Returns the cumulative amount of time spent accessing the database.
     *
     * @deprecated Use {@link mondrian.server.monitor.Monitor#getServer()} and
     *  {@link mondrian.server.monitor.ServerInfo#sqlStatementExecuteNanos};
     *  will be removed in 4.0.
     */
    public static long dbTimeMillis() {
        return databaseMillis;
    }

    /**
     * Adds to the cumulative amount of time spent accessing the database.
     *
     * @deprecated Will be removed in 4.0.
     */
    public static void addDatabaseTime(long millis) {
        databaseMillis += millis;
    }

    /**
     * Returns the system time less the time spent accessing the database.
     * Use this method to figure out how long an operation took: call this
     * method before an operation and after an operation, and the difference
     * is the amount of non-database time spent.
     *
     * @deprecated Will be removed in 4.0.
     */
    public static long nonDbTimeMillis() {
        final long systemMillis = System.currentTimeMillis();
        return systemMillis - databaseMillis;
    }

    /**
     * Creates a very simple implementation of {@link Validator}. (Only
     * useful for resolving trivial expressions.)
     */
    public static Validator createSimpleValidator(final FunTable funTable) {
        return new Validator() {
            public Query getQuery() {
                return null;
            }

            public SchemaReader getSchemaReader() {
                throw new UnsupportedOperationException();
            }

            public Exp validate(Exp exp, boolean scalar) {
                return exp;
            }

            public void validate(ParameterExpr parameterExpr) {
            }

            public void validate(MemberProperty memberProperty) {
            }

            public void validate(QueryAxis axis) {
            }

            public void validate(Formula formula) {
            }

            public FunDef getDef(Exp[] args, String name, Syntax syntax) {
                // Very simple resolution. Assumes that there is precisely
                // one resolver (i.e. no overloading) and no argument
                // conversions are necessary.
                List<Resolver> resolvers = funTable.getResolvers(name, syntax);
                final Resolver resolver = resolvers.get(0);
                final List<Resolver.Conversion> conversionList =
                    new ArrayList<Resolver.Conversion>();
                final FunDef def =
                    resolver.resolve(args, this, conversionList);
                assert conversionList.isEmpty();
                return def;
            }

            public boolean alwaysResolveFunDef() {
                return false;
            }

            public boolean canConvert(
                int ordinal, Exp fromExp,
                int to,
                List<Resolver.Conversion> conversions)
            {
                return true;
            }

            public boolean requiresExpression() {
                return false;
            }

            public FunTable getFunTable() {
                return funTable;
            }

            public Parameter createOrLookupParam(
                boolean definition,
                String name,
                Type type,
                Exp defaultExp,
                String description)
            {
                return null;
            }
        };
    }

    /**
     * Reads a Reader until it returns EOF and returns the contents as a String.
     *
     * @param rdr  Reader to Read.
     * @param bufferSize size of buffer to allocate for reading.
     * @return content of Reader as String
     * @throws IOException on I/O error
     */
    public static String readFully(final Reader rdr, final int bufferSize)
        throws IOException
    {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException(
                "Buffer size must be greater than 0");
        }

        final char[] buffer = new char[bufferSize];
        final StringBuilder buf = new StringBuilder(bufferSize);

        int len;
        while ((len = rdr.read(buffer)) != -1) {
            buf.append(buffer, 0, len);
        }
        return buf.toString();
    }

    /**
     * Reads an input stream until it returns EOF and returns the contents as an
     * array of bytes.
     *
     * @param in  Input stream
     * @param bufferSize size of buffer to allocate for reading.
     * @return content of stream as an array of bytes
     * @throws IOException on I/O error
     */
    public static byte[] readFully(final InputStream in, final int bufferSize)
        throws IOException
    {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException(
                "Buffer size must be greater than 0");
        }

        final byte[] buffer = new byte[bufferSize];
        final ByteArrayOutputStream baos =
            new ByteArrayOutputStream(bufferSize);

        int len;
        while ((len = in.read(buffer)) != -1) {
            baos.write(buffer, 0, len);
        }
        return baos.toByteArray();
    }

    /**
     * Reads an InputStream into an OutputStream until it returns EOF.
     *
     * @param is Input stream
     * @param bufferSize size of buffer to allocate for reading.
     * @param os Output stream
     * @throws IOException on IO error
     */
    public static void readFully(
        final InputStream is,
        final int bufferSize,
        ByteArrayOutputStream os)
        throws IOException
    {
        final byte[] buffer = new byte[bufferSize];
        int len = is.read(buffer);
        while (len != -1) {
            os.write(buffer, 0, len);
            len = is.read(buffer);
        }
    }

    /**
     * Returns the contents of a URL, substituting tokens.
     *
     * <p>Replaces the tokens "${key}" if the map is not null and "key" occurs
     * in the key-value map.
     *
     * <p>If the URL string starts with "inline:" the contents are the
     * rest of the URL.
     *
     * @param urlStr  URL string
     * @param map Key/value map
     * @return Contents of URL with tokens substituted
     * @throws IOException on I/O error
     */
    public static String readURL(final String urlStr, Map<String, String> map)
        throws IOException
    {
        if (urlStr.startsWith("inline:")) {
            String content = urlStr.substring("inline:".length());
            if (map != null) {
                content = Util.replaceProperties(content, map);
            }
            return content;
        } else {
            final URL url = new URL(urlStr);
            return readURL(url, map);
        }
    }

    /**
     * Returns the contents of a URL.
     *
     * @param url URL
     * @return Contents of URL
     * @throws IOException on I/O error
     */
    public static String readURL(final URL url) throws IOException {
        return readURL(url, null);
    }

    /**
     * Returns the contents of a URL, substituting tokens.
     *
     * <p>Replaces the tokens "${key}" if the map is not null and "key" occurs
     * in the key-value map.
     *
     * @param url URL
     * @param map Key/value map
     * @return Contents of URL with tokens substituted
     * @throws IOException on I/O error
     */
    public static String readURL(
        final URL url,
        Map<String, String> map)
        throws IOException
    {
        final Reader r =
            new BufferedReader(new InputStreamReader(url.openStream()));
        final int BUF_SIZE = 8096;
        try {
            String xmlCatalog = readFully(r, BUF_SIZE);
            xmlCatalog = Util.replaceProperties(xmlCatalog, map);
            return xmlCatalog;
        } finally {
            r.close();
        }
    }

    /**
     * Gets content via Apache VFS. File must exist and have content
     *
     * @param url URL String
     * @return Contents of file as an input stream
     * @throws java.io.IOException on error
     */
    public static InputStream readVirtualFile(String url)
        throws IOException
    {
        return VIRTUAL_FILE_HANDLER.get().readVirtualFile(url);
    }

    public static String readVirtualFileAsString(
        String catalogUrl)
        throws IOException
    {
        InputStream in = readVirtualFile(catalogUrl);
        try {
            return IOUtils.toString(in);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    /**
     * Converts a {@link Properties} object to a string-to-string {@link Map}.
     *
     * @param properties Properties
     * @return String-to-string map
     */
    public static Map<String, String> toMap(final Properties properties) {
        return new AbstractMap<String, String>() {
            @SuppressWarnings({"unchecked"})
            public Set<Entry<String, String>> entrySet() {
                return (Set) properties.entrySet();
            }
        };
    }
    /**
     * Replaces tokens in a string.
     *
     * <p>Replaces the tokens "${key}" if "key" occurs in the key-value map.
     * Otherwise "${key}" is left in the string unchanged.
     *
     * @param text Source string
     * @param env Map of key-value pairs
     * @return String with tokens substituted
     */
    public static String replaceProperties(
        String text,
        Map<String, String> env)
    {
        // As of JDK 1.5, cannot use StringBuilder - appendReplacement requires
        // the antediluvian StringBuffer.
        StringBuffer buf = new StringBuffer(text.length() + 200);

        Pattern pattern = Pattern.compile("\\$\\{([^${}]+)\\}");
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            String varName = matcher.group(1);
            String varValue = env.get(varName);
            if (varValue != null) {
                matcher.appendReplacement(buf, varValue);
            } else {
                matcher.appendReplacement(buf, "\\${$1}");
            }
        }
        matcher.appendTail(buf);

        return buf.toString();
    }

    public static String printMemory() {
        return printMemory(null);
    }

    public static String printMemory(String msg) {
        final Runtime rt = Runtime.getRuntime();
        final long freeMemory = rt.freeMemory();
        final long totalMemory = rt.totalMemory();
        final StringBuilder buf = new StringBuilder(64);

        buf.append("FREE_MEMORY:");
        if (msg != null) {
            buf.append(msg);
            buf.append(':');
        }
        buf.append(' ');
        buf.append(freeMemory / 1024);
        buf.append("kb ");

        long hundredths = (freeMemory * 10000) / totalMemory;

        buf.append(hundredths / 100);
        hundredths %= 100;
        if (hundredths >= 10) {
            buf.append('.');
        } else {
            buf.append(".0");
        }
        buf.append(hundredths);
        buf.append('%');

        return buf.toString();
    }

    /**
     * Casts a Set to a Set with a different element type.
     *
     * @param set Set
     * @return Set of desired type
     */
    @SuppressWarnings({"unchecked"})
    public static <T> Set<T> cast(Set<?> set) {
        return (Set<T>) set;
    }

    /**
     * Casts a List to a List with a different element type.
     *
     * @param list List
     * @return List of desired type
     */
    @SuppressWarnings({"unchecked"})
    public static <T> List<T> cast(List<?> list) {
        return (List<T>) list;
    }

    /**
     * Returns whether it is safe to cast a collection to a collection with a
     * given element type.
     *
     * @param collection Collection
     * @param clazz Target element type
     * @param <T> Element type
     * @return Whether all not-null elements of the collection are instances of
     *   element type
     */
    public static <T> boolean canCast(
        Collection<?> collection,
        Class<T> clazz)
    {
        for (Object o : collection) {
            if (o != null && !clazz.isInstance(o)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Casts a collection to iterable.
     *
     * Under JDK 1.4, {@link Collection} objects do not implement
     * {@link Iterable}, so this method inserts a casting wrapper. (Since
     * Iterable does not exist under JDK 1.4, they will have been compiled
     * under JDK 1.5 or later, then retrowoven to 1.4 class format. References
     * to Iterable will have been replaced with references to
     * <code>com.rc.retroweaver.runtime.Retroweaver_</code>.
     *
     * <p>Under later JDKs this method is trivial. This method can be deleted
     * when we discontinue support for JDK 1.4.
     *
     * @param iterable Object which ought to be iterable
     * @param <T> Element type
     * @return Object cast to Iterable
     */
    public static <T> Iterable<T> castToIterable(
        final Object iterable)
    {
        if (Util.Retrowoven
            && !(iterable instanceof Iterable))
        {
            return new Iterable<T>() {
                public Iterator<T> iterator() {
                    return ((Collection<T>) iterable).iterator();
                }
            };
        }
        return (Iterable<T>) iterable;
    }

    /**
     * Makes a collection of untyped elements appear as a list of strictly typed
     * elements, by filtering out those which are not of the correct type.
     *
     * <p>The returned object is an {@link Iterable},
     * which makes it ideal for use with the 'foreach' construct. For example,
     *
     * <blockquote><code>List&lt;Number&gt; numbers = Arrays.asList(1, 2, 3.14,
     * 4, null, 6E23);<br/>
     * for (int myInt : filter(numbers, Integer.class)) {<br/>
     * &nbsp;&nbsp;&nbsp;&nbsp;print(i);<br/>
     * }</code></blockquote>
     *
     * will print 1, 2, 4.
     *
     * @param iterable Source iterable
     * @param includeFilter Class whose instances to return
     * @return Iterable that only returns those elements that are instances
     * of {@code includeFilter}.
     */
    public static <E> Iterable<E> filter(
        final Iterable<?> iterable,
        final Class<E> includeFilter)
    {
        return new Iterable<E>() {
            public Iterator<E> iterator()
            {
                return new Filterator<E>(iterable.iterator(), includeFilter);
            }
        };
    }

    public static <E> Iterable<E> filter(
        final Object[] array,
        final Class<E> includeFilter)
    {
        return filter(Arrays.asList(array), includeFilter);
    }

    /**
     * Looks up an enumeration by name, returning null if null or not valid.
     *
     * @param clazz Enumerated type
     * @param name Name of constant
     */
    public static <E extends Enum<E>> E lookup(Class<E> clazz, String name) {
        return lookup(clazz, name, null);
    }

    /**
     * Looks up an enumeration by name, returning a given default value if null
     * or not valid. Default value must not be null.
     *
     * @param name Name of constant
     * @param defaultValue Default value if constant is not found
     * @return Value, or null if name is null or value does not exist
     */
    public static <E extends Enum<E>> E lookup(
        String name, E defaultValue)
    {
        Class<E> clazz = (Class<E>) defaultValue.getClass();
        while (!clazz.isEnum()) {
            // Enum constants with overriding methods belong to their own
            // anonymous subclass.
            clazz = (Class<E>) clazz.getSuperclass();
        }
        return lookup(clazz, name, defaultValue);
    }

    /**
     * Looks up an enumeration by name, returning a given default value if null
     * or not valid.
     *
     * @param clazz Enumerated type
     * @param name Name of constant
     * @param defaultValue Default value if constant is not found
     * @return Value, or null if name is null or value does not exist
     */
    public static <E extends Enum<E>> E lookup(
        Class<E> clazz,
        String name,
        E defaultValue)
    {
        if (name == null) {
            return defaultValue;
        }
        try {
            return Enum.valueOf(clazz, name);
        } catch (IllegalArgumentException e) {
            return defaultValue;
        }
    }

    /**
     * Make a BigDecimal from a double. On JDK 1.5 or later, the BigDecimal
     * precision reflects the precision of the double while with JDK 1.4
     * this is not the case.
     *
     * @param d the input double
     * @return the BigDecimal
     */
    public static BigDecimal makeBigDecimalFromDouble(double d) {
        return compatible.makeBigDecimalFromDouble(d);
    }

    /**
     * Returns a literal pattern String for the specified String.
     *
     * <p>Specification as for {@link Pattern#quote(String)}, which was
     * introduced in JDK 1.5.
     *
     * @param s The string to be literalized
     * @return A literal string replacement
     */
    public static String quotePattern(String s) {
        return compatible.quotePattern(s);
    }

    /**
     * Generates a unique id.
     *
     * <p>From JDK 1.5 onwards, uses a {@code UUID}.
     *
     * @return A unique id
     */
    public static String generateUuidString() {
        return compatible.generateUuidString();
    }

    /**
     * Compiles a script to yield a Java interface.
     *
     * <p>Only valid JDK 1.6 and higher; fails on JDK 1.5 and earlier.</p>
     *
     * @param iface Interface script should implement
     * @param script Script code
     * @param engineName Name of engine (e.g. "JavaScript")
     * @param <T> Interface
     * @return Object that implements given interface
     */
    public static <T> T compileScript(
        Class<T> iface,
        String script,
        String engineName)
    {
        return compatible.compileScript(iface, script, engineName);
    }

    /**
     * Removes a thread local from the current thread.
     *
     * <p>From JDK 1.5 onwards, calls {@link ThreadLocal#remove()}; before
     * that, no-ops.</p>
     *
     * @param threadLocal Thread local
     * @param <T> Type
     */
    public static <T> void threadLocalRemove(ThreadLocal<T> threadLocal) {
        compatible.threadLocalRemove(threadLocal);
    }

    /**
     * Creates a hash set that, like {@link java.util.IdentityHashMap},
     * compares keys using identity.
     *
     * @param <T> Element type
     * @return Set
     */
    public static <T> Set<T> newIdentityHashSet() {
        return compatible.newIdentityHashSet();
    }

    /**
     * Creates a new udf instance from the given udf class.
     *
     * @param udfClass the class to create new instance for
     * @param functionName Function name, or null
     * @return an instance of UserDefinedFunction
     */
    public static UserDefinedFunction createUdf(
        Class<? extends UserDefinedFunction> udfClass,
        String functionName)
    {
        // Instantiate class with default constructor.
        UserDefinedFunction udf;
        String className = udfClass.getName();
        String functionNameOrEmpty =
            functionName == null
                ? ""
                : functionName;

        // Find a constructor.
        Constructor<?> constructor;
        Object[] args = {};

        // 0. Check that class is public and top-level or static.
        // Before JDK 1.5, inner classes are impossible; retroweaver cannot
        // handle the getEnclosingClass method, so skip the check.
        if (!Modifier.isPublic(udfClass.getModifiers())
            || (!PreJdk15
                && udfClass.getEnclosingClass() != null
                && !Modifier.isStatic(udfClass.getModifiers())))
        {
            throw MondrianResource.instance().UdfClassMustBePublicAndStatic.ex(
                functionName,
                className);
        }

        // 1. Look for a constructor "public Udf(String name)".
        try {
            constructor = udfClass.getConstructor(String.class);
            if (Modifier.isPublic(constructor.getModifiers())) {
                args = new Object[] {functionName};
            } else {
                constructor = null;
            }
        } catch (NoSuchMethodException e) {
            constructor = null;
        }
        // 2. Otherwise, look for a constructor "public Udf()".
        if (constructor == null) {
            try {
                constructor = udfClass.getConstructor();
                if (Modifier.isPublic(constructor.getModifiers())) {
                    args = new Object[] {};
                } else {
                    constructor = null;
                }
            } catch (NoSuchMethodException e) {
                constructor = null;
            }
        }
        // 3. Else, no constructor suitable.
        if (constructor == null) {
            throw MondrianResource.instance().UdfClassWrongIface.ex(
                functionNameOrEmpty,
                className,
                UserDefinedFunction.class.getName());
        }
        // Instantiate class.
        try {
            udf = (UserDefinedFunction) constructor.newInstance(args);
        } catch (InstantiationException e) {
            throw MondrianResource.instance().UdfClassWrongIface.ex(
                functionNameOrEmpty,
                className, UserDefinedFunction.class.getName());
        } catch (IllegalAccessException e) {
            throw MondrianResource.instance().UdfClassWrongIface.ex(
                functionName,
                className,
                UserDefinedFunction.class.getName());
        } catch (ClassCastException e) {
            throw MondrianResource.instance().UdfClassWrongIface.ex(
                functionNameOrEmpty,
                className,
                UserDefinedFunction.class.getName());
        } catch (InvocationTargetException e) {
            throw MondrianResource.instance().UdfClassWrongIface.ex(
                functionName,
                className,
                UserDefinedFunction.class.getName());
        }

        return udf;
    }

    /**
     * Check the resultSize against the result limit setting. Throws
     * LimitExceededDuringCrossjoin exception if limit exceeded.
     *
     * When it is called from RolapNativeSet.checkCrossJoin(), it is only
     * possible to check the known input size, because the final CJ result
     * will come from the DB(and will be checked against the limit when
     * fetching from the JDBC result set, in SqlTupleReader.prepareTuples())
     *
     * @param resultSize Result limit
     * @throws ResourceLimitExceededException
     */
    public static void checkCJResultLimit(long resultSize) {
        int resultLimit = MondrianProperties.instance().ResultLimit.get();

        // Throw an exeption, if the size of the crossjoin exceeds the result
        // limit.
        if (resultLimit > 0 && resultLimit < resultSize) {
            throw MondrianResource.instance().LimitExceededDuringCrossjoin.ex(
                resultSize, resultLimit);
        }

        // Throw an exception if the crossjoin exceeds a reasonable limit.
        // (Yes, 4 billion is a reasonable limit.)
        if (resultSize > Integer.MAX_VALUE) {
            throw MondrianResource.instance().LimitExceededDuringCrossjoin.ex(
                resultSize, Integer.MAX_VALUE);
        }
    }

    /**
     * Converts an olap4j connect string into a legacy mondrian connect string.
     *
     * <p>For example,
     * "jdbc:mondrian:Datasource=jdbc/SampleData;
     * Catalog=foodmart/FoodMart.mondrian.xml;"
     * becomes
     * "Provider=Mondrian;
     * Datasource=jdbc/SampleData;Catalog=foodmart/FoodMart.mondrian.xml;"
     *
     * <p>This method is intended to allow legacy applications (such as JPivot
     * and Mondrian's XMLA server) to continue to create connections using
     * Mondrian's legacy connection API even when they are handed an olap4j
     * connect string.
     *
     * @param url olap4j connect string
     * @return mondrian connect string, or null if cannot be converted
     */
    public static String convertOlap4jConnectStringToNativeMondrian(
        String url)
    {
        if (url.startsWith("jdbc:mondrian:")) {
            return "Provider=Mondrian; "
                + url.substring("jdbc:mondrian:".length());
        }
        return null;
    }

    /**
     * Checks if a String is whitespace, empty ("") or null.</p>
     *
     * <pre>
     * StringUtils.isBlank(null) = true
     * StringUtils.isBlank("") = true
     * StringUtils.isBlank(" ") = true
     * StringUtils.isBlank("bob") = false
     * StringUtils.isBlank(" bob ") = false
     * </pre>
     *
     * <p>(Copied from commons-lang.)
     *
     * @param str the String to check, may be null
     * @return <code>true</code> if the String is null, empty or whitespace
     */
    public static boolean isBlank(String str) {
        final int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a role which has access to everything.
     * @param schema A schema to bind this role to.
     * @return A role with root access to the schema.
     */
    public static Role createRootRole(Schema schema) {
        RoleImpl role = new RoleImpl();
        role.grant(schema, Access.ALL);
        role.makeImmutable();
        return role;
    }

    /** Returns a view of an array of {@link Named} objects as a
     * {@link NamedList}. */
    public static <T extends Named> NamedList<T> asNamedList(final T... ts) {
        return new AbstractNamedList<T>() {
            public T get(int index) {
                return ts[index];
            }

            public int size() {
                return ts.length;
            }

            public String getName(Object element) {
                return ((Named) element).getName();
            }
        };
    }

    /**
     * Similar to {@link ClassLoader#getResource(String)}, except the lookup
     *  is in reverse order.<br>
     *  i.e. returns the resource from the supplied classLoader or the
     *  one closest to it in the hierarchy, instead of the closest to the root
     *  class loader
     * @param classLoader The class loader to fetch from
     * @param name The resource name
     * @return A URL object for reading the resource, or null if the resource
     * could not be found or the invoker doesn't have adequate privileges to get
     * the resource.
     * @see ClassLoader#getResource(String)
     * @see ClassLoader#getResources(String)
     */
    public static URL getClosestResource(ClassLoader classLoader, String name) {
        URL resource = null;
        try {
            // The last resource will be from the nearest ClassLoader.
            Enumeration<URL> resourceCandidates =
                classLoader.getResources(name);
            while (resourceCandidates.hasMoreElements()) {
                resource = resourceCandidates.nextElement();
            }
        } catch (IOException ioe) {
            // ignore exception - it's OK if file is not found
            // just keep getResource contract and return null
            Util.discard(ioe);
        }
        return resource;
    }

    public static abstract class AbstractFlatList<T>
        implements List<T>, RandomAccess
    {
        protected final List<T> asArrayList() {
            //noinspection unchecked
            return Arrays.asList((T[]) toArray());
        }

        public Iterator<T> iterator() {
            return asArrayList().iterator();
        }

        public ListIterator<T> listIterator() {
            return asArrayList().listIterator();
        }

        public boolean isEmpty() {
            return false;
        }

        public boolean add(Object t) {
            throw new UnsupportedOperationException();
        }

        public boolean addAll(Collection<? extends T> c) {
            throw new UnsupportedOperationException();
        }

        public boolean addAll(int index, Collection<? extends T> c) {
            throw new UnsupportedOperationException();
        }

        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        public void clear() {
            throw new UnsupportedOperationException();
        }

        public T set(int index, Object element) {
            throw new UnsupportedOperationException();
        }

        public void add(int index, Object element) {
            throw new UnsupportedOperationException();
        }

        public T remove(int index) {
            throw new UnsupportedOperationException();
        }

        public ListIterator<T> listIterator(int index) {
            return asArrayList().listIterator(index);
        }

        public List<T> subList(int fromIndex, int toIndex) {
            return asArrayList().subList(fromIndex, toIndex);
        }

        public boolean contains(Object o) {
            return indexOf(o) >= 0;
        }

        public boolean containsAll(Collection<?> c) {
            Iterator<?> e = c.iterator();
            while (e.hasNext()) {
                if (!contains(e.next())) {
                    return false;
                }
            }
            return true;
        }

        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * List that stores its two elements in the two members of the class.
     * Unlike {@link java.util.ArrayList} or
     * {@link java.util.Arrays#asList(Object[])} there is
     * no array, only one piece of memory allocated, therefore is very compact
     * and cache and CPU efficient.
     *
     * <p>The list is read-only, cannot be modified or resized, and neither
     * of the elements can be null.
     *
     * <p>The list is created via {@link Util#flatList(Object[])}.
     *
     * @see mondrian.olap.Util.Flat3List
     * @param <T>
     */
    protected static class Flat2List<T>
        extends AbstractFlatList<T>
        implements Comparable<T>
    {
        private final T t0;
        private final T t1;

        Flat2List(T t0, T t1) {
            this.t0 = t0;
            this.t1 = t1;
            assert t0 != null;
            assert t1 != null;
        }

        public String toString() {
            return "[" + t0 + ", " + t1 + "]";
        }

        public T get(int index) {
            switch (index) {
            case 0:
                return t0;
            case 1:
                return t1;
            default:
                throw new IndexOutOfBoundsException("index " + index);
            }
        }

        public int size() {
            return 2;
        }

        public Iterator<T> iterator() {
            return Arrays.asList(t0, t1).iterator();
        }

        public boolean equals(Object o) {
            if (o instanceof Flat2List) {
                Flat2List that = (Flat2List) o;
                return Util.equals(this.t0, that.t0)
                    && Util.equals(this.t1, that.t1);
            }
            return Arrays.asList(t0, t1).equals(o);
        }

        public int hashCode() {
            int h = 1;
            h = h * 31 + t0.hashCode();
            h = h * 31 + t1.hashCode();
            return h;
        }

        public int indexOf(Object o) {
            if (t0.equals(o)) {
                return 0;
            }
            if (t1.equals(o)) {
                return 1;
            }
            return -1;
        }

        public int lastIndexOf(Object o) {
            if (t1.equals(o)) {
                return 1;
            }
            if (t0.equals(o)) {
                return 0;
            }
            return -1;
        }

        @SuppressWarnings({"unchecked"})
        public <T2> T2[] toArray(T2[] a) {
            a[0] = (T2) t0;
            a[1] = (T2) t1;
            return a;
        }

        public Object[] toArray() {
            return new Object[] {t0, t1};
        }

        public int compareTo(T o) {
            //noinspection unchecked
            Flat2List<T> that = (Flat2List<T>) o;
            int c = Util.compare((Comparable) t0, (Comparable) that.t0);
            if (c != 0) {
                return c;
            }
            return Util.compare((Comparable) t1, (Comparable) that.t1);
        }
    }

    /**
     * List that stores its three elements in the three members of the class.
     * Unlike {@link java.util.ArrayList} or
     * {@link java.util.Arrays#asList(Object[])} there is
     * no array, only one piece of memory allocated, therefore is very compact
     * and cache and CPU efficient.
     *
     * <p>The list is read-only, cannot be modified or resized, and none
     * of the elements can be null.
     *
     * <p>The list is created via {@link Util#flatList(Object[])}.
     *
     * @see mondrian.olap.Util.Flat2List
     * @param <T>
     */
    protected static class Flat3List<T>
        extends AbstractFlatList<T>
        implements Comparable<T>
    {
        private final T t0;
        private final T t1;
        private final T t2;

        Flat3List(T t0, T t1, T t2) {
            this.t0 = t0;
            this.t1 = t1;
            this.t2 = t2;
            assert t0 != null;
            assert t1 != null;
            assert t2 != null;
        }

        public String toString() {
            return "[" + t0 + ", " + t1 + ", " + t2 + "]";
        }

        public T get(int index) {
            switch (index) {
            case 0:
                return t0;
            case 1:
                return t1;
            case 2:
                return t2;
            default:
                throw new IndexOutOfBoundsException("index " + index);
            }
        }

        public int size() {
            return 3;
        }

        public Iterator<T> iterator() {
            return Arrays.asList(t0, t1, t2).iterator();
        }

        public boolean equals(Object o) {
            if (o instanceof Flat3List) {
                Flat3List that = (Flat3List) o;
                return Util.equals(this.t0, that.t0)
                    && Util.equals(this.t1, that.t1)
                    && Util.equals(this.t2, that.t2);
            }
            return o.equals(this);
        }

        public int hashCode() {
            int h = 1;
            h = h * 31 + t0.hashCode();
            h = h * 31 + t1.hashCode();
            h = h * 31 + t2.hashCode();
            return h;
        }

        public int indexOf(Object o) {
            if (t0.equals(o)) {
                return 0;
            }
            if (t1.equals(o)) {
                return 1;
            }
            if (t2.equals(o)) {
                return 2;
            }
            return -1;
        }

        public int lastIndexOf(Object o) {
            if (t2.equals(o)) {
                return 2;
            }
            if (t1.equals(o)) {
                return 1;
            }
            if (t0.equals(o)) {
                return 0;
            }
            return -1;
        }

        @SuppressWarnings({"unchecked"})
        public <T2> T2[] toArray(T2[] a) {
            a[0] = (T2) t0;
            a[1] = (T2) t1;
            a[2] = (T2) t2;
            return a;
        }

        public Object[] toArray() {
            return new Object[] {t0, t1, t2};
        }

        public int compareTo(T o) {
            //noinspection unchecked
            Flat3List<T> that = (Flat3List<T>) o;
            int c = Util.compare((Comparable) t0, (Comparable) that.t0);
            if (c != 0) {
                return c;
            }
            c = Util.compare((Comparable) t1, (Comparable) that.t1);
            if (c != 0) {
                return c;
            }
            return Util.compare((Comparable) t2, (Comparable) that.t2);
        }
    }

    /**
     * Garbage-collecting iterator. Iterates over a collection of references,
     * and if any of the references has been garbage-collected, removes it from
     * the collection.
     *
     * @param <T> Element type
     */
    public static class GcIterator<T> implements Iterator<T> {
        private final Iterator<? extends Reference<T>> iterator;
        private boolean hasNext;
        private T next;

        public GcIterator(Iterator<? extends Reference<T>> iterator) {
            this.iterator = iterator;
            this.hasNext = true;
            moveToNext();
        }

        /**
         * Creates an iterator over a collection of references.
         *
         * @param referenceIterable Collection of references
         * @param <T2> element type
         * @return iterable over collection
         */
        public static <T2> Iterable<T2> over(
            final Iterable<? extends Reference<T2>> referenceIterable)
        {
            return new Iterable<T2>() {
                public Iterator<T2> iterator() {
                    return new GcIterator<T2>(referenceIterable.iterator());
                }
            };
        }

        private void moveToNext() {
            while (iterator.hasNext()) {
                final Reference<T> ref = iterator.next();
                next = ref.get();
                if (next != null) {
                    return;
                }
                iterator.remove();
            }
            hasNext = false;
        }

        public boolean hasNext() {
            return hasNext;
        }

        public T next() {
            final T next1 = next;
            moveToNext();
            return next1;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Function that takes zero arguments and returns {@code RT}.
     *
     * @param <RT> Return type
     */
    public static interface Function0<RT> {
        RT apply();
    }

    /**
     * Function that takes one argument ({@code PT}) and returns {@code RT}.
     *
     * @param <RT> Return type
     * @param <PT> Parameter type
     */
    public static interface Function1<PT, RT> {
        RT apply(PT param);
    }

    /**
     * Predicate that takes one argument ({@code PT}).
     * Can be used as a {@code Functor1&lt;PT&gt;} or as an Apache-collections
     * Predicate.
     *
     * @param <PT> Parameter type
     */
    public static abstract class Predicate1<PT>
        implements Predicate, Function1<PT, Boolean>
    {
        public Boolean apply(PT param) {
            return test(param);
        }

        public boolean evaluate(Object o) {
            //noinspection unchecked
            return test((PT) o);
        }

        public abstract boolean test(PT pt);
    }

    public static <T> Function1<T, T> identityFunctor() {
        //noinspection unchecked
        return (Function1) IDENTITY_FUNCTION;
    }

    private static final Function1 IDENTITY_FUNCTION =
        new Function1<Object, Object>() {
            public Object apply(Object param) {
                return param;
            }
        };

    /**
     * Returns a predicate that takes 1 argument and always returns true.
     *
     * @param <PT> Parameter type
     * @return Predicate that always returns true
     */
    public static <PT> Predicate1<PT> truePredicate1() {
        //noinspection unchecked
        return (Predicate1) TRUE_PREDICATE1;
    }

    private static final Predicate1 TRUE_PREDICATE1 =
        new Predicate1<Object>() {
            public boolean test(Object o) {
                return true;
            }
        };

    /**
     * Information about memory usage.
     *
     * @see mondrian.olap.Util#getMemoryInfo()
     */
    public interface MemoryInfo {
        Usage get();

        public interface Usage {
            long getUsed();
            long getCommitted();
            long getMax();
        }
    }

    /**
     * A {@link Comparator} implementation which can deal
     * correctly with {@link RolapUtil#sqlNullValue}.
     */
    public static class SqlNullSafeComparator
        implements Comparator<Comparable>
    {
        public static final SqlNullSafeComparator instance =
            new SqlNullSafeComparator();

        private SqlNullSafeComparator() {
        }

        public int compare(Comparable o1, Comparable o2) {
            if (o1 == RolapUtil.sqlNullValue) {
                return -1;
            }
            if (o2 == RolapUtil.sqlNullValue) {
                return 1;
            }
            //noinspection unchecked
            return o1.compareTo(o2);
        }
    }

    private static class ComparableEmptyList
        extends AbstractList
        implements Comparable<List>
    {
        private ComparableEmptyList() {
        }

        public Object get(int index) {
            throw new IndexOutOfBoundsException();
        }

        public int size() {
            return 0;
        }

        public int compareTo(List o) {
            if (o == this) {
                return 0;
            }
            return o.size() == 0 ? 0 : -1;
        }
    }

    private static class ComparableList<T extends Comparable<T>>
        extends AbstractList<T>
        implements Comparable<List<T>>
    {
        private final List<T> list;

        protected ComparableList(List<T> list) {
            this.list = list;
        }

        public T get(int index) {
            return list.get(index);
        }

        public int size() {
            return list.size();
        }

        public int compareTo(List<T> o) {
            int size = size();
            if (o.size() == size) {
                return compare(this, o, size);
            }
            final int c = compare(this, o, Math.min(size(), o.size()));
            if (c != 0) {
                return c;
            }
            return size() - o.size();
        }

        private static <T extends Comparable>
        int compare(List<T> list0, List<T> list1, int size) {
            for (int i = 0; i < size; i++) {
                Comparable o0 = list0.get(i);
                Comparable o1 = list1.get(i);
                int c = o0.compareTo(o1);
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }
    }

    /**
     * This class implements the Knuth-Morris-Pratt algorithm
     * to search within a byte array for a token byte array.
     */
    public static class ByteMatcher {
        private final int[] matcher;
        public final byte[] key;
        public ByteMatcher(byte[] key) {
            this.key = key;
            this.matcher = compile(key);
        }
        /**
         * Matches the pre-compiled byte array token against a
         * byte array variable and returns the index of the key
         * within the array.
         * @param a An array of bytes to search for.
         * @return -1 if not found, or the index (0 based) of the match.
         */
        public int match(byte[] a) {
            int j = 0;
            for (int i = 0; i < a.length; i++) {
                while (j > 0 && key[j] != a[i]) {
                    j = matcher[j - 1];
                }
                if (a[i] == key[j]) {
                    j++;
                }
                if (key.length == j) {
                    return
                        i - key.length + 1;
                }
            }
            return -1;
        }
        private int[] compile(byte[] key) {
            int[] matcher = new int[key.length];
            int j = 0;
            for (int i = 1; i < key.length; i++) {
                while (j > 0 && key[j] != key[i]) {
                    j = matcher[j - 1];
                }
                if (key[i] == key[j]) {
                    j++;
                }
                matcher[i] = j;
            }
            return matcher;
        }
    }
}

// End Util.java
