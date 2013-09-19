/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.net.URL;
import java.util.*;

/**
 * Utility functions to discover Java services.
 *
 * <p>Java services are described in
 * <a href="http://java.sun.com/j2se/1.5.0/docs/guide/jar/jar.html#Service%20Provider">
 * the JAR File Specification</a>.
 *
 * <p>Based on the suggested file format, this class reads the service
 * entries in a JAR file and discovers implementors of an interface.
 *
 * @author Marc Batchelor
 */
public class ServiceDiscovery<T> {

    private static final Log logger = LogFactory.getLog(ServiceDiscovery.class);

    private final Class<T> theInterface;

    /**
     * Creates a ServiceDiscovery.
     *
     * @param theInterface Interface for service
     *
     * @return ServiceDiscovery for finding instances of the given interface
     */
    public static <T> ServiceDiscovery<T> forClass(Class<T> theInterface) {
        return new ServiceDiscovery<T>(theInterface);
    }

    /**
     * Creates a ServiceDiscovery.
     *
     * @param theInterface Interface for service
     */
    private ServiceDiscovery(Class<T> theInterface) {
        assert theInterface != null;
        this.theInterface = theInterface;
    }

    /**
     * Returns a list of classes that implement the service.
     *
     * @return List of classes that implement the service
     */
    public List<Class<T>> getImplementor() {
        // Use linked hash set to eliminate duplicates but still return results
        // in the order they were added.
        Set<Class<T>> uniqueClasses = new LinkedHashSet<Class<T>>();

        ClassLoader cLoader = Thread.currentThread().getContextClassLoader();
        if (cLoader == null) {
            cLoader = this.getClass().getClassLoader();
        }
        try {
            // Enumerate the files because I may have more than one .jar file
            // that contains an implementation for the interface, and therefore,
            // more than one list of entries.
            String lookupName = "META-INF/services/" + theInterface.getName();
            Enumeration<URL> urlEnum = cLoader.getResources(lookupName);
            while (urlEnum.hasMoreElements()) {
                URL resourceURL = urlEnum.nextElement();
                InputStream is = null;
                try {
                    is = resourceURL.openStream();
                    BufferedReader reader =
                        new BufferedReader(new InputStreamReader(is));

                    // read each class and parse it
                    String clazz;
                    while ((clazz = reader.readLine()) != null) {
                        parseImplementor(clazz, cLoader, uniqueClasses);
                    }
                } catch (IOException e) {
                    logger.warn(
                        "Error while finding service file " + resourceURL
                        + " for " + theInterface,
                        e);
                } finally {
                    if (is != null) {
                        is.close();
                    }
                }
            }
        } catch (IOException e) {
            logger.warn(
                "Error while finding service files for " + theInterface,
                e);
        }
        List<Class<T>> rtn = new ArrayList<Class<T>>();
        rtn.addAll(uniqueClasses);
        return rtn;
    }

    /**
     * Parses a list of classes that implement a service.
     *
     * @param clazz Class name (or list of class names)
     * @param cLoader Class loader
     * @param uniqueClasses Set of classes (output)
     */
    protected void parseImplementor(
        String clazz,
        ClassLoader cLoader,
        Set<Class<T>> uniqueClasses)
    {
        // Split should leave me with a class name in the first string
        // which will nicely ignore comments in the line. I checked
        // and found that it also doesn't choke if:
        // a- There are no spaces on the line - you end up with one entry
        // b- A line begins with a whitespace character (the trim() fixes that)
        // c- Multiples of the same interface are filtered out
        assert clazz != null;

        String[] classList = clazz.trim().split("#");
        String theClass = classList[0].trim(); // maybe overkill, maybe not. :-D
        if (theClass.length() == 0) {
            // Ignore lines that are empty after removing comments & trailing
            // spaces.
            return;
        }
        try {
            // I want to look up the class but not cause the static
            // initializer to execute.
            Class interfaceImplementor =
                Class.forName(theClass, false, cLoader);
            if (theInterface.isAssignableFrom(interfaceImplementor)) {
                //noinspection unchecked
                uniqueClasses.add((Class<T>) interfaceImplementor);
            } else {
                logger.error(
                    "Class " + interfaceImplementor
                    + " cannot be assigned to interface "
                    + theInterface);
            }
        } catch (ClassNotFoundException ignored) {
            ignored.printStackTrace();
        } catch (LinkageError ignored) {
            // including ExceptionInInitializerError
            ignored.printStackTrace();
        }
    }
}

// End ServiceDiscovery.java
