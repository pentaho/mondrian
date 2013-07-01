/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2005-2009 Pentaho and others
// All Rights Reserved.
*/
package mondrian.test;

import junit.framework.*;

import java.lang.reflect.Constructor;
import java.util.Enumeration;

/**
 * Simple test runner.
 */
public class SimpleTestRunner {
    protected static void usage(String msg) {
        StringBuilder buf = new StringBuilder(64);
        if (msg != null) {
            buf.append(msg);
            buf.append('\n');
        }
        buf.append("Usage: mondrian.test.SimpleTestRunner options tests*");
        buf.append('\n');
        buf.append("  options:");
        buf.append('\n');
        buf.append("     -h     (print this text)");
        buf.append('\n');
        buf.append("     -q     (error output quiet)");
        buf.append('\n');
        buf.append("  tests:");
        buf.append('\n');
        buf.append("     -c testcaseclassname methodnames*");
        buf.append('\n');
        buf.append("If no method names are given, then all are tested");
        buf.append('\n');

        System.out.println(buf.toString());
        System.exit(0);
    }

    protected static TestCase makeTestCase(String classname) throws Exception {
        Class cls = Class.forName(classname);

        return (TestCase) cls.newInstance();
    }

    protected static TestCase makeTestCase(String classname, String methodname)
        throws Exception
    {
        Class cls = Class.forName(classname);

        Constructor cons = cls.getConstructor(new Class[] { String.class});
        return (TestCase) cons.newInstance(new Object[] { methodname });
    }

    protected static void outputErrorInfo(Enumeration e, boolean quiet) {
        while (e.hasMoreElements()) {
            TestFailure tf = (TestFailure) e.nextElement();
            if (! quiet) {
                System.out.println(tf.trace());
                Throwable t = tf.thrownException().getCause();
                while (t != null) {
                    t.printStackTrace();
                    t = t.getCause();
                }
            } else {
                System.out.println(tf.toString());
                System.out.println("run without -quiet for more information");
            }
        }
    }
    public static void main(String[] args) throws Throwable {
        String classname = null;
        TestCase testcase = null;
        boolean quiet = false;
        boolean explicitMethods = false;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            if (arg.equals("-h")) {
                usage(null);
            } else if (arg.equals("-q")) {
                quiet = true;
            } else if (arg.equals("-quiet")) {
                quiet = true;
            } else if (arg.equals("-c")) {
                i++;
                if (i == args.length) {
                    usage("Must supply TestCase classname after -c");
                }
                classname = args[i];
            } else {
                explicitMethods = true;
                String methodname = arg;

                if (testcase == null) {
                    if (classname == null) {
                        usage(
                            "Must supply TestCase classname before methodname");
                    }
                    testcase = makeTestCase(classname, methodname);
                } else {
                    testcase.setName(methodname);
                }
                //testcase.runBare();
                junit.framework.TestResult tr = testcase.run();
                System.out.println("Test Class: " + classname);
                System.out.println("  Method : " + methodname);
                System.out.println("  Error Count : " + tr.errorCount());
                if (tr.errorCount() != 0) {
                    Enumeration e = tr.errors();
                    outputErrorInfo(e, quiet);
                }
                System.out.println("  Failure Count : " + tr.failureCount());
                if (tr.failureCount() != 0) {
                    Enumeration e = tr.failures();
                    outputErrorInfo(e, quiet);
                }
                testcase = null;
            }
        }
        if (! explicitMethods) {
            if (classname == null) {
                usage("Must supply TestCase classname");
            }
            if (testcase == null) {
                try {
                    // maybe it has a no argument constructor
                    testcase = makeTestCase(classname);
                } catch (InstantiationException ex) {
                    String msg =
                        "InstantiationException: "
                        + "most likely the test class does not have a "
                        + "zero-parameter, public constructor.";
                    System.out.println(msg);
                    System.exit(1);
                } catch (Exception ex) {
                    testcase = null;
                    // ignore
                }
            }
            if (testcase != null) {
                TestSuite suite = new TestSuite(testcase.getClass());
                TestResult tr = new TestResult();

                suite.run(tr);
                System.out.println("Test Class: " + classname);
                System.out.println("  Method Count : " + tr.runCount());
                System.out.println("  Error Count : " + tr.errorCount());
                if (tr.errorCount() != 0) {
                    Enumeration e = tr.errors();
                    outputErrorInfo(e, quiet);
                }
                System.out.println("  Failure Count : " + tr.failureCount());
                if (tr.failureCount() != 0) {
                    Enumeration e = tr.failures();
                    outputErrorInfo(e, quiet);
                }
            }
        }
    }
}

// End SimpleTestRunner.java
