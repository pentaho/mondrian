/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 19 September, 2002
*/

package mondrian.resource;

import java.util.*;
import java.io.InputStream;
import java.io.IOException;

/**
 * <code>ShadowResourceBundle</code> is an abstract base class for
 * {@link ResourceBundle} classes which are backed by a properties file. When
 * the class is created, it loads a properties file with the same name as the
 * class.
 *
 * <p> In the standard scheme (see {@link ResourceBundle}),
 * if you call <code>{@link ResourceBundle#getBundle}("foo.MyResource")</code>,
 * it first looks for a class called <code>foo.MyResource</code>, then
 * looks for a file called <code>foo/MyResource.properties</code>. If it finds
 * the file, it creates a {@link PropertyResourceBundle} and loads the class.
 * The problem is if you want to load the <code>.properties</code> file
 * into a dedicated class; <code>ShadowResourceBundle</code> helps with this
 * case.
 *
 * <p> You should create a class as follows:<blockquote>
 *
 * <pre>package foo;
 *class MyResource extends mondrian.resource.ShadowResourceBundle {
 *    public MyResource() throws java.io.IOException {
 *    }
 *}</pre>
 *
 * </blockquote> Then when you call
 * {@link ResourceBundle#getBundle ResourceBundle.getBundle("foo.MyResource")},
 * it will find the class before the properties file, but still automatically
 * load the properties file based upon the name of the class.
 */
public abstract class ShadowResourceBundle extends ResourceBundle {
	private PropertyResourceBundle bundle;
	private static final HashMap mapThreadToLocale = new HashMap();
	protected static final Object[] emptyObjectArray = new Object[0];

	/**
	 * Creates a <code>ShadowResourceBundle</code>, and reads resources from
	 * a <code>.properties</code> file with the same name as the current class.
	 * For example, if the class is called <code>foo.MyResource_en_US</code>,
	 * reads from <code>foo/MyResource_en_US.properties</code>.
	 */
	protected ShadowResourceBundle() throws IOException {
		super();
		final InputStream stream = openPropertiesFile(getClass());
		if (stream == null) {
			throw new IOException("could not open properties file for " + getClass());
		}
		bundle = new PropertyResourceBundle(stream);
		stream.close();
	}

	/**
	 * Opens the properties file corresponding to a given class. The code is
	 * copied from {@link ResourceBundle}.
	 */
	private static InputStream openPropertiesFile(Class clazz) {
		final ClassLoader loader = clazz.getClassLoader();
		final String resName = clazz.getName().replace('.', '/') + ".properties";
        return (InputStream)java.security.AccessController.doPrivileged(
            new java.security.PrivilegedAction() {
                public Object run() {
                    if (loader != null) {
                        return loader.getResourceAsStream(resName);
                    } else {
                        return ClassLoader.getSystemResourceAsStream(resName);
                    }
                }
            }
        );
	}

	public Enumeration getKeys() {
		return bundle.getKeys();
	}

	protected Object handleGetObject(String key)
			throws MissingResourceException {
		return bundle.getObject(key);
	}

	/**
	 * Returns the instance of the <code>baseName</code> resource bundle for
	 * the current thread's locale. For example, if called with
	 * "mondrian.olap.MondrianResource", from a thread which has called {@link
	 * #setThreadLocale}({@link Locale#FRENCH}), will get an instance of
	 * "mondrian.olap.MondrianResource_FR" from the cache.
	 *
	 * <p> This method should be called from a derived class, with the proper
	 * casting:<blockquote>
	 *
	 * <pre>class MyResource extends ShadowResourceBundle {
	 *    ...
	 *    /&#42;&#42;
	 *      &#42; Retrieves the instance of {&#64;link MyResource} appropriate
	 *      &#42; to the current locale. If this thread has specified a locale
	 *      &#42; by calling {&#64;link #setThreadLocale}, this locale is used,
	 *      &#42; otherwise the default locale is used.
	 *      &#42;&#42;/
	 *    public static MyResource instance() {
	 *       return (MyResource) instance(MyResource.class.getName());
	 *    }
	 *    ...
	 * }</pre></blockquote>
	 */
	protected static ResourceBundle instance(String baseName) {
		return instance(baseName, getThreadLocale());
	}
	/**
	 * Returns the instance of the <code>baseName</code> resource bundle
	 * for the given locale.
	 *
	 * <p> This method should be called from a derived class, with the proper
	 * casting:<blockquote>
	 *
	 * <pre>class MyResource extends ShadowResourceBundle {
	 *    ...
	 *
	 *    /&#42;&#42;
	 *      &#42; Retrieves the instance of {&#64;link MyResource} appropriate
	 *      &#42; to the given locale.
	 *      &#42;&#42;/
	 *    public static MyResource instance(Locale locale) {
	 *       return (MyResource) instance(MyResource.class.getName(), locale);
	 *    }
	 *    ...
	 * }</pre></blockquote>
	 */
	protected static ShadowResourceBundle instance(
			String baseName, Locale locale) {
		if (locale == null) {
			locale = Locale.getDefault();
		}
		ResourceBundle bundle = ResourceBundle.getBundle(baseName, locale);
		if (bundle instanceof PropertyResourceBundle) {
			throw new ClassCastException(
					"ShadowResourceBundle.instance('" + baseName + "','" +
					locale + "') found " +
					baseName + "_" + locale + ".properties but not " +
					baseName + "_" + locale + ".class");
		}
		return (ShadowResourceBundle) bundle;
	}

	/** Sets the locale for the current thread. Used by {@link
	 * #instance(String,Locale)}. **/
	public static void setThreadLocale(Locale locale) {
		mapThreadToLocale.put(Thread.currentThread(), locale);
	}

	/** Returns the preferred locale of the current thread, or null if the
	 * thread has not called {@link #setThreadLocale}. **/
	public static Locale getThreadLocale() {
		return (Locale) mapThreadToLocale.get(Thread.currentThread());
	}
}

// End ShadowResourceBundle.java
