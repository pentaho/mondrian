/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2013 Pentaho
// All Rights Reserved.
*/
package mondrian.test;

import mondrian.olap.Util;

import junit.framework.*;

import org.apache.commons.collections.map.ReferenceMap;

import org.eigenbase.xom.XMLOutput;

import org.w3c.dom.*;
import org.xml.sax.SAXException;

import java.io.*;
import java.net.URL;
import java.util.*;
import javax.xml.parsers.*;

/**
 * A collection of resources used by tests.
 *
 * <p>Loads files containing test input and output into memory.
 * If there are differences, writes out a log file containing the actual
 * output.
 *
 * <p>Typical usage is as follows:<ol>
 * <li>A testcase class defines a method<blockquote><code><pre>
 *
 * package com.acme.test;
 *
 * public class MyTest extends TestCase {
 *     public DiffRepository getDiffRepos() {
 *         return DiffRepository.lookup(MyTest.class);
 *     }
 *
 *     public void testToUpper() {
 *          getDiffRepos().assertEquals("${result}", "${string}");
 *     }

 *     public void testToLower() {
 *          getDiffRepos().assertEquals("Multi-line\nstring", "${string}");
 *     }
 * }</pre></code></blockquote>
 *
 * There is an accompanying reference file named after the class,
 * <code>com/acme/test/MyTest.ref.xml</code>:
 * <blockquote><code><pre>
 * &lt;Root&gt;
 *     &lt;TestCase name="testToUpper"&gt;
 *         &lt;Resource name="string"&gt;
 *             &lt;![CDATA[String to be converted to upper case]]&gt;
 *         &lt;/Resource&gt;
 *         &lt;Resource name="result"&gt;
 *             &lt;![CDATA[STRING TO BE CONVERTED TO UPPER CASE]]&gt;
 *         &lt;/Resource&gt;
 *     &lt;/TestCase&gt;
 *     &lt;TestCase name="testToLower"&gt;
 *         &lt;Resource name="result"&gt;
 *             &lt;![CDATA[multi-line
 * string]]&gt;
 *         &lt;/Resource&gt;
 *     &lt;/TestCase&gt;
 * &lt;/Root&gt;
 * </pre></code></blockquote>
 *
 * <p>If any of the testcases fails, a log file is generated, called
 * <code>com/acme/test/MyTest.log.xml</code> containing the actual output.
 * The log file is otherwise identical to the reference log, so once the
 * log file has been verified, it can simply be copied over to become the new
 * reference log.</p>
 *
 * <p>If a resource or testcase does not exist, <code>DiffRepository</code>
 * creates them in the log file. Because DiffRepository is so forgiving, it is
 * very easy to create new tests and testcases.</p>
 *
 * <p>The {@link #lookup} method ensures that all test cases share the same
 * instance of the repository. This is important more than one one test case
 * fails. The shared instance ensures that the generated <code>.log.xml</code>
 * file contains the actual for <em>both</em> test cases.
 *
 * @author jhyde
 */
public class DiffRepository
{
    private final DiffRepository baseRepos;
    private final DocumentBuilder docBuilder;
    private Document doc;
    private final Element root;
    private final File refFile;
    private final File logFile;

/*
    Example XML document:

    <Root>
        <TestCase name="testFoo">
            <Resource name="sql">
                <![CDATA[select * from emps]]>
            </Resource>
            <Resource name="plan">
                <![CDATA[MockTableImplRel.FENNEL_EXEC(table=[SALES, EMP])]]>
            </Resource>
        </TestCase>
        <TestCase name="testBar">
            <Resource name="sql">
                <![CDATA[select * from depts where deptno = 10]]>
            </Resource>
            <Resource name="output">
                <![CDATA[10, 'Sales']]>
            </Resource>
        </TestCase>
    </Root>
*/

    private static final String RootTag = "Root";
    private static final String TestCaseTag = "TestCase";
    private static final String TestCaseNameAttr = "name";
    private static final String ResourceTag = "Resource";
    private static final String ResourceNameAttr = "name";
    private static final String ResourceSqlDialectAttr = "dialect";

    private static final ThreadLocal<String> CurrentTestCaseName =
        new ThreadLocal<String>();

    /**
     * Holds one diff-repository per class. It is necessary for all testcases
     * in the same class to share the same diff-repository: if the
     * repos gets loaded once per testcase, then only one diff is recorded.
     */
    private static final Map<Class, DiffRepository> mapClassToRepos =
        new ReferenceMap(ReferenceMap.SOFT, ReferenceMap.SOFT);

    /**
     * Default prefix directories.
     */
    private static final List<String> JAVA_PREFIXES =
        Arrays.asList("src", "test", "java");

    private static final List<String> RESOURCE_PREFIXES =
        Arrays.asList("src", "test", "resources");

    private static File findFile(
        Class clazz, List<String> javaPrefixes, List<String> resourcePrefixes,
        String suffix)
    {
        // The reference file for class "com.foo.Bar" is "com/foo/Bar.ref.xml"
        String rest =
            clazz.getName().replace('.', File.separatorChar) + suffix;
        File fileBase = getFileBase(clazz, javaPrefixes, resourcePrefixes);
        return new File(fileBase, rest);
    }

    /**
     * Returns the base directory relative to which test logs are stored.
     *
     * <p>Deduces the directory based upon the current directory.
     * If the current directory is "/home/jhyde/open/mondrian/intellij",
     * returns "/home/jhyde/open/mondrian/src/test/resources".
     */
    private static File getFileBase(
        Class clazz, List<String> javaPrefixes, List<String> resourcePrefixes)
    {
        String javaFileName =
            clazz.getName().replace('.', File.separatorChar) + ".java";
        File file = new File(System.getProperty("user.dir"));
        while (true) {
            File file2 = plus(javaPrefixes, file);
            if (file2.isDirectory()
                && new File(file2, javaFileName).exists())
            {
                return plus(resourcePrefixes, file);
            }

            file = file.getParentFile();
            if (file == null) {
                throw new RuntimeException("cannot find base dir");
            }
        }
    }

    private static File plus(List<String> prefixes, File file) {
        File file2 = file;
        for (String prefix : prefixes) {
            file2 = new File(file2, prefix);
        }
        return file2;
    }

    /**
     * Creates a DiffRepository from a pair of files.
     *
     * @param refFile File containing reference results
     * @param logFile File to contain the actual output of the test run
     * @param baseRepos Base repository to inherit from, or null
     */
    public DiffRepository(
        File refFile, File logFile, DiffRepository baseRepos)
    {
        this.baseRepos = baseRepos;
        if (refFile == null) {
            throw new IllegalArgumentException("url must not be null");
        }
        this.refFile = refFile;
        Util.discard(this.refFile);
        this.logFile = logFile;

        // Load the document.
        DocumentBuilderFactory fac = DocumentBuilderFactory.newInstance();
        try {
            this.docBuilder = fac.newDocumentBuilder();
            if (refFile.exists()) {
                // Parse the reference file.
                this.doc = docBuilder.parse(new FileInputStream(refFile));
                // Don't write a log file yet -- as far as we know, it's still
                // identical.
            } else {
                // There's no reference file. Create and write a log file.
                this.doc = docBuilder.newDocument();
                this.doc.appendChild(
                    doc.createElement(RootTag));
                flushDoc();
            }
            this.root = doc.getDocumentElement();
            if (!root.getNodeName().equals(RootTag)) {
                throw new RuntimeException(
                    "expected root element of type '" + RootTag
                    + "', but found '" + root.getNodeName() + "'");
            }
        } catch (ParserConfigurationException e) {
            throw Util.newInternal(e, "error while creating xml parser");
        } catch (IOException e) {
            throw Util.newInternal(e, "error while creating xml parser");
        } catch (SAXException e) {
            throw Util.newInternal(e, "error while creating xml parser");
        }
    }

    /**
     * Creates a read-only repository reading from a URL.
     *
     * @param refUrl URL pointing to reference file
     */
    public DiffRepository(URL refUrl)
    {
        this.refFile = null;
        this.logFile = null;
        this.baseRepos = null;

        // Load the document.
        DocumentBuilderFactory fac = DocumentBuilderFactory.newInstance();
        try {
            this.docBuilder = fac.newDocumentBuilder();
            // Parse the reference file.
            this.doc = docBuilder.parse(refUrl.openStream());
            this.root = doc.getDocumentElement();
            if (!root.getNodeName().equals(RootTag)) {
                throw new RuntimeException(
                    "expected root element of type '" + RootTag
                    + "', but found '" + root.getNodeName() + "'");
            }
        } catch (ParserConfigurationException e) {
            throw Util.newInternal(e, "error while creating xml parser");
        } catch (IOException e) {
            throw Util.newInternal(e, "error while creating xml parser");
        } catch (SAXException e) {
            throw Util.newInternal(e, "error while creating xml parser");
        }
    }

    /**
     * Expands a string containing one or more variables.
     * (Currently only works if there is one variable.)
     */
    public String expand(String tag, String text)
    {
        if (text == null) {
            return null;
        } else if (text.startsWith("${")
            && text.endsWith("}"))
        {
            final String testCaseName = getCurrentTestCaseName(true);
            final String token = text.substring(2, text.length() - 1);
            if (tag == null) {
                tag = token;
            }
            assert token.startsWith(tag)
                : "token '" + token
                + "' does not match tag '" + tag + "'";
            final String expanded = get(testCaseName, token);
            if (expanded == null) {
                // Token is not specified. Return the original text: this will
                // cause a diff, and the actual value will be written to the
                // log file.
                return text;
            }
            return expanded;
        } else {
            // Make sure what appears in the resource file is consistent with
            // what is in the Java. It helps to have a redundant copy in the
            // resource file.
            final String testCaseName = getCurrentTestCaseName(true);
            if (baseRepos != null
                && baseRepos.get(testCaseName, tag) != null)
            {
                // set in base repos; don't override
            } else {
                set(tag, text);
            }
            return text;
        }
    }

    /**
     * Sets the value of a given resource of the current testcase.
     *
     * @param resourceName Name of the resource, e.g. "sql"
     * @param value Value of the resource
     */
    public synchronized void set(String resourceName, String value)
    {
        assert resourceName != null;
        final String testCaseName = getCurrentTestCaseName(true);
        update(testCaseName, resourceName, value);
    }

    public void amend(String expected, String actual)
    {
        if (expected.startsWith("${")
            && expected.endsWith("}"))
        {
            String token = expected.substring(2, expected.length() - 1);
            set(token, actual);
        } else {
            // do nothing
        }
    }

    public synchronized String get(
        final String testCaseName,
        String resourceName)
    {
        return get(testCaseName, resourceName, null);
    }

    /**
     * Returns a given resource from a given testcase.
     *
     * @param testCaseName Name of test case, e.g. "testFoo"
     * @param resourceName Name of resource, e.g. "sql", "plan"
     * @param dialectName Name of sql dialect, e.g. "MYSQL", "LUCIDDB"
     * @return The value of the resource, or null if not found
     */
    public synchronized String get(
        final String testCaseName,
        String resourceName,
        String dialectName)
    {
        Element testCaseElement =
            getTestCaseElement(root, testCaseName);
        if (testCaseElement == null) {
            if (baseRepos != null) {
                return baseRepos.get(testCaseName, resourceName, dialectName);
            } else {
                return null;
            }
        }
        final Element resourceElement =
            getResourceElement(testCaseElement, resourceName, dialectName);
        if (resourceElement != null) {
            return getText(resourceElement);
        }
        return null;
    }

    /**
     * Returns the text under an element.
     */
    private static String getText(Element element)
    {
        // If there is a <![CDATA[ ... ]]> child, return its text and ignore
        // all other child elements.
        final NodeList childNodes = element.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node node = childNodes.item(i);
            if (node instanceof CDATASection) {
                return node.getNodeValue();
            }
        }
        // Otherwise return all the text under this element (including
        // whitespace).
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node node = childNodes.item(i);
            if (node instanceof Text) {
                buf.append(((Text) node).getData());
            }
        }
        return buf.toString();
    }

    /**
     * Returns the &lt;TestCase&gt; element corresponding to the current
     * test case.
     *
     * @param root Root element of the document
     * @param testCaseName Name of test case
     * @return TestCase element, or null if not found
     */
    private static Element getTestCaseElement(
        final Element root,
        final String testCaseName)
    {
        final NodeList childNodes = root.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node child = childNodes.item(i);
            if (child.getNodeName().equals(TestCaseTag)) {
                Element testCase = (Element) child;
                if (testCaseName.equals(
                        testCase.getAttribute(TestCaseNameAttr)))
                {
                    return testCase;
                }
            }
        }
        return null;
    }

    /**
     * @return a list of the names of all test cases defined in the
     * repository file
     */
    public List<String> getTestCaseNames() {
        List<String> list = new ArrayList<String>();
        final NodeList childNodes = root.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node child = childNodes.item(i);
            if (child.getNodeName().equals(TestCaseTag)) {
                Element testCase = (Element) child;
                list.add(testCase.getAttribute(TestCaseNameAttr));
            }
        }
        return list;
    }

    /**
     * Sets the name of the current test case.  For use in
     * tests created via dynamic suite() methods.  Caller should pass
     * test case name from setUp(), and null from tearDown() to clear.
     *
     * @param testCaseName name of test case to set as current,
     * or null to clear
     */
    public void setCurrentTestCaseName(String testCaseName)
    {
        CurrentTestCaseName.set(testCaseName);
    }

    /**
     * Returns the name of the current testcase by looking up the call
     * stack for a method whose name starts with "test", for example
     * "testFoo".
     *
     * @param fail Whether to fail if no method is found
     * @return Name of current testcase, or null if not found
     */
    public String getCurrentTestCaseName(boolean fail)
    {
        // check thread-local first
        String testCaseName = CurrentTestCaseName.get();
        if (testCaseName != null) {
            return testCaseName;
        }

        // Clever, this. Dump the stack and look up it for a method which
        // looks like a testcase name, e.g. "testFoo".
        final StackTraceElement[] stackTrace;
        //noinspection ThrowableInstanceNeverThrown
        Throwable runtimeException = new Throwable();
        runtimeException.fillInStackTrace();
        stackTrace = runtimeException.getStackTrace();
        for (StackTraceElement stackTraceElement : stackTrace) {
            final String methodName = stackTraceElement.getMethodName();
            if (methodName.startsWith("test")) {
                return methodName;
            }
        }
        if (fail) {
            throw new RuntimeException("no testcase on current callstack");
        } else {
            return null;
        }
    }

    /**
     * Calls {@link #assertEquals(String, String, String, mondrian.olap.Util.Function1)}
     * with a null filter.
     *
     * @param tag Tag
     * @param expected Expected value (may contain a "${tag}")
     * @param actual Actual value
     */
    public final void assertEquals(
        String tag, String expected, String actual)
    {
        assertEquals(tag, expected, actual, null);
    }

    public void assertEquals(
        String tag,
        String expected,
        String actual,
        Util.Function1<String, String> filter)
    {
        final String testCaseName = getCurrentTestCaseName(true);
        String expected2 = expand(tag, expected);
        if (expected2 == null) {
            update(testCaseName, expected, actual);
            throw new AssertionFailedError(
                "reference file does not contain resource '" + expected
                + "' for testcase '" + testCaseName + "'");
        } else {
            final String expected3;
            if (filter != null) {
                expected3 = filter.apply(expected2);
            } else {
                expected3 = expected2;
            }
            try {
                // TODO jvs 25-Apr-2006:  reuse bulk of
                // DiffTestCase.diffTestLog here; besides newline
                // insensitivity, it can report on the line
                // at which the first diff occurs, which is useful
                // for largish snippets
                String expected2Canonical =
                    Util.replace(expected3, Util.nl, "\n");
                String actualCanonical = Util.replace(actual, Util.nl, "\n");
                Assert.assertEquals(
                    expected2Canonical,
                    actualCanonical);
            } catch (ComparisonFailure e) {
                amend(expected, actual);
                throw e;
            }
        }
    }

    /**
     * Creates a new document with a given resource.
     *
     * <p>This method is synchronized, in case two threads are running
     * test cases of this test at the same time.
     *
     * @param testCaseName Test case name
     * @param resourceName Resource name
     * @param value New value of resource
     */
    private synchronized void update(
        String testCaseName,
        String resourceName,
        String value)
    {
        Element testCaseElement =
            getTestCaseElement(root, testCaseName);
        if (testCaseElement == null) {
            testCaseElement = doc.createElement(TestCaseTag);
            testCaseElement.setAttribute(TestCaseNameAttr, testCaseName);
            root.appendChild(testCaseElement);
        }
        Element resourceElement =
            getResourceElement(testCaseElement, resourceName);
        if (resourceElement == null) {
            resourceElement = doc.createElement(ResourceTag);
            resourceElement.setAttribute(ResourceNameAttr, resourceName);
            testCaseElement.appendChild(resourceElement);
        } else {
            removeAllChildren(resourceElement);
        }
        resourceElement.appendChild(doc.createCDATASection(value));

        // Write out the document.
        flushDoc();
    }

    /**
     * Flush the reference document to the file system.
     */
    private void flushDoc()
    {
        FileWriter w = null;
        try {
            w = new FileWriter(logFile);
            write(doc, w);
        } catch (IOException e) {
            throw Util.newInternal(
                e,
                "error while writing test reference log '" + logFile + "'");
        } finally {
            if (w != null) {
                try {
                    w.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Returns a given resource from a given testcase.
     *
     * @param testCaseElement  The enclosing TestCase element,
     *          e.g. <code>&lt;TestCase name="testFoo"&gt;</code>.
     * @param resourceName Name of resource, e.g. "sql", "plan"
     * @return The value of the resource, or null if not found
     */
    private static Element getResourceElement(
        Element testCaseElement,
        String resourceName)
    {
        return getResourceElement(testCaseElement, resourceName, null);
    }

    private static Element getResourceElement(
        Element testCaseElement,
        String resourceName,
        String resourceAttribute1)
    {
        final NodeList childNodes = testCaseElement.getChildNodes();
        for (int i = 0; i < childNodes.getLength(); i++) {
            Node child = childNodes.item(i);
            if (child.getNodeName().equals(ResourceTag)
                && resourceName.equals(
                    ((Element) child).getAttribute(ResourceNameAttr))
                && ((resourceAttribute1 == null)
                    || resourceAttribute1.equals(
                        ((Element) child).getAttribute(
                            ResourceSqlDialectAttr))))
            {
                return (Element) child;
            }
        }
        return null;
    }

    private static void removeAllChildren(Element element)
    {
        final NodeList childNodes = element.getChildNodes();
        while (childNodes.getLength() > 0) {
            element.removeChild(childNodes.item(0));
        }
    }

    /**
     * Serializes an XML document as text.
     *
     * <p>FIXME: I'm sure there's a library call to do this, but I'm danged
     * if I can find it. -- jhyde, 2006/2/9.
     */
    private static void write(Document doc, Writer w)
    {
        final XMLOutput out = new XMLOutput(w);
        out.setIndentString("    ");
        writeNode(doc, out);
    }

    private static void writeNode(Node node, XMLOutput out)
    {
        final NodeList childNodes;
        switch (node.getNodeType()) {
        case Node.DOCUMENT_NODE:
            out.print("<?xml version=\"1.0\" ?>" + Util.nl);
            childNodes = node.getChildNodes();
            for (int i = 0; i < childNodes.getLength(); i++) {
                Node child = childNodes.item(i);
                writeNode(child, out);
            }
//            writeNode(((Document) node).getDocumentElement(), out);
            break;

        case Node.ELEMENT_NODE:
            Element element = (Element) node;
            final String tagName = element.getTagName();
            out.beginBeginTag(tagName);
            // Attributes.
            final NamedNodeMap attributeMap = element.getAttributes();
            for (int i = 0; i < attributeMap.getLength(); i++) {
                final Node att = attributeMap.item(i);
                out.attribute(att.getNodeName(), att.getNodeValue());
            }
            out.endBeginTag(tagName);
            // Write child nodes, ignoring attributes but including text.
            childNodes = node.getChildNodes();
            for (int i = 0; i < childNodes.getLength(); i++) {
                Node child = childNodes.item(i);
                if (child.getNodeType() == Node.ATTRIBUTE_NODE) {
                    continue;
                }
                writeNode(child, out);
            }
            out.endTag(tagName);
            break;

        case Node.ATTRIBUTE_NODE:
            out.attribute(node.getNodeName(), node.getNodeValue());
            break;

        case Node.CDATA_SECTION_NODE:
            CDATASection cdata = (CDATASection) node;
            out.cdata(cdata.getNodeValue(), true);
            break;

        case Node.TEXT_NODE:
            Text text = (Text) node;
            final String wholeText = text.getNodeValue();
            if (!isWhitespace(wholeText)) {
                out.cdata(wholeText, false);
            }
            break;

        case Node.COMMENT_NODE:
            Comment comment = (Comment) node;
            out.print("<!--" + comment.getNodeValue() + "-->" + Util.nl);
            break;

        default:
            throw new RuntimeException(
                "unexpected node type: " + node.getNodeType()
                + " (" + node + ")");
        }
    }

    /**
     * Returns whether a given piece of text is solely whitespace.
     *
     * @param text Text
     * @return Whether text is whitespace
     */
    private static boolean isWhitespace(String text)
    {
        for (int i = 0, count = text.length(); i < count; ++i) {
            final char c = text.charAt(i);
            switch (c) {
            case ' ':
            case '\t':
            case '\n':
                break;
            default:
                return false;
            }
        }
        return true;
    }

    /**
     * Finds the repository instance for a given class, using the default
     * prefixes, and with no parent repository.
     *
     * @see #lookup(Class, DiffRepository, java.util.List, java.util.List)
     */
    public static DiffRepository lookup(Class clazz)
    {
        return lookup(clazz, null, JAVA_PREFIXES, RESOURCE_PREFIXES);
    }

    /**
     * Finds the repository instance for a given class.
     *
     * <p>It is important that all testcases in a class share the same
     * repository instance. This ensures that, if two or more testcases fail,
     * the log file will contains the actual results of both testcases.
     *
     * <p>The <code>baseRepos</code> parameter is useful if the test is an
     * extension to a previous test. If the test class has a base class which
     * also has a repository, specify the repository here.  DiffRepository will
     * look for resources in the base class if it cannot find them in this
     * repository. If test resources from testcases in the base class are
     * missing or incorrect, it will not write them to the log file -- you
     * probably need to fix the base test.
     *
     * @param clazz Testcase class
     * @param baseRepos Base class of test class
     * @param prefixes Array of directory names to look in; if null, the
     *   default {"testsrc", "main"} is used
     * @return The diff repository shared between testcases in this class.
     */
    public static DiffRepository lookup(
        Class clazz, DiffRepository baseRepos, List<String> prefixes,
        List<String> resourcePrefixes)
    {
        DiffRepository diffRepos = mapClassToRepos.get(clazz);
        if (diffRepos == null) {
            if (prefixes == null) {
                prefixes = JAVA_PREFIXES;
            }
            final File refFile =
                findFile(clazz, prefixes, resourcePrefixes, ".ref.xml");
            final File logFile =
                findFile(clazz, prefixes, resourcePrefixes, ".log.xml");
            diffRepos = new DiffRepository(refFile, logFile, baseRepos);
            mapClassToRepos.put(clazz, diffRepos);
        }
        return diffRepos;
    }
}

// End DiffRepository.java
