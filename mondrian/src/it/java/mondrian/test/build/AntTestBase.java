/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2008 SQLstream, Inc.
// Copyright (C) 2009-2010 Pentaho
// All Rights Reserved.
*/

package mondrian.test.build;

import mondrian.olap.Util;

import junit.framework.TestCase;

import java.io.*;

/**
 * Base class for tests that execute Ant targets.  Sub-classes
 * should invoke {@link #runAntTest(String)} to run an Ant target.  If
 * the Ant sub-process cannot be started of if it returns an exit code that
 * indicates error, the test fails.
 *
 * <p>
 * AntTestBase makes the following assumptions about its run-time environment:
 * <ol>
 *   <li>Ant can be invoked by executing <code>ant</code>.  That is, ant is
 *       on the current PATH.</li>
 *   <li>The version of Ant on the PATH is new enough to execute the
 *       build.xml script.</li>
 *   <li>The test is being invoked in the root directory (e.g.
 *       //open/mondrian) as the current directory or a subdirectory of it.</li>
 * </ol>
 *
 * <pre>
 * REVIEW: SWZ: 3/11/2006: This class is not portable to Windows.  Potential
 * solutions:
 * 1) Check for Windows via System properties and invoke
 *    "command.com ant.bat [target]" (or whatever's necessary) when the OS is
 *    Windows.
 * 2) Require Ant libraries be on the classpath and invoke Ant's API
 *    directly.  This is preferred, since it should be OS neutral.
 * </pre>
 *
 * @author Stephan Zuercher
 * @since Mar 11, 2006
 */
abstract class AntTestBase extends TestCase
{
    private static final boolean DEBUG = false;

    /**
     * Creates an AntTestBase.
     *
     * @param name Test name
     */
    AntTestBase(String name)
    {
        super(name);
    }

    /**
     * Runs an ant task.
     *
     * @param target Name of ant target
     * @throws IOException
     * @throws InterruptedException
     */
    protected void runAntTest(String target)
        throws IOException, InterruptedException
    {
        // On hudson, ant is not on the path but is at /opt/ant1.7. If that
        // file exists, assume that we are on hudson. Otherwise, require ant
        // to be on the path.
        String antCommand = "ant";
        final String[] paths = {
            "/opt/ant1.7/bin/ant",
            "/opt/apache-ant-1.7.1/bin/ant"
        };
        for (String path : paths) {
            final File antFile = new File(path);
            if (antFile.exists()) {
                antCommand = antFile.getAbsolutePath();
                break;
            }
        }

        Runtime runtime = Runtime.getRuntime();
        Process proc =
            runtime.exec(
                new String[] { antCommand, "-find", "build.xml", target });

        final Sucker outSucker =
            new Sucker(proc.getInputStream(), DEBUG ? System.out : null);
        final Sucker errSucker =
            new Sucker(proc.getErrorStream(), DEBUG ? System.err : null);
        outSucker.start();
        errSucker.start();

        int exitCode = proc.waitFor();
        if (exitCode != 0) {
            final String lineSep = System.getProperty("line.separator");
            fail(
                "Error running 'ant " + target + "'" + lineSep
                + "Stdout:" + lineSep
                + outSucker.toString()
                + "Stderr:" + lineSep
                + errSucker.toString());
        }
    }

    /**
     * Thread that reads from an input stream, stores in a buffer, and also
     * writes to a given output stream.
     *
     * <p>Useful for ensuring that processes don't hang up because one of their
     * outputs (stdout or stderr) is full.
     */
    private static class Sucker extends Thread {
        private final InputStream stream;
        private final PrintStream out;
        private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        /**
         * Creates a Sucker.
         *
         * @param stream Input stream
         * @param out Output stream
         */
        Sucker(InputStream stream, PrintStream out) {
            this.stream = stream;
            this.out = out;
        }

        public void run() {
            byte[] buf = new byte[1000];
            int x;
            try {
                while ((x = stream.read(buf)) >= 0) {
                    baos.write(buf, 0, x);
                    if (out != null) {
                        out.write(buf, 0, x);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public String toString() {
            return baos.toString();
        }
    }
}

// End AntTestBase.java
