/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 8 February, 2002
*/

package mondrian.resource;
import java.util.Hashtable;
import java.util.Locale;

/**
 * <code>ResourceBase</code> is a skeleton implementation of {@link Resource}.
 *
 * @author jhyde
 * @since 8 February, 2002
 * @version $Id$
 **/
public abstract class ResourceBase {
	private Locale locale;
	private mondrian.resource.ResourceDef.BaflResourceList resourceList;
	private Hashtable mapCodeToError;
	private Hashtable mapNameToError;

	// implement Resource
	public void init(java.net.URL url, Locale locale)
		throws java.io.IOException
	{
		ResourceDef.BaflResourceList resourceList =
			mondrian.resource.Util.load(url);
		init(resourceList, locale);
	}

	// implement Resource
	public void init(ResourceDef.BaflResourceList resourceList, Locale locale)
	{
		if (locale != null && resourceList.locale != null) {
			assert(
				locale.toString().equals(resourceList.locale),
				"Resource file locale '" + resourceList.locale +
				"' does not match requested locale '" + locale.toString() +
				"'");
		}
		this.locale = locale;
		this.mapCodeToError = new Hashtable();
		this.mapNameToError = new Hashtable();
		for (int i = 0; i < resourceList.resources.length; i++) {
			ResourceDef.BaflResourceText res = resourceList.resources[i];
			this.mapCodeToError.put(res.id, res);
			this.mapNameToError.put(res.macroName, res);
		}
	}

	// implement Resource
	public Locale getLocale() {
		return locale;
	}
	// implement Resource
	public String formatError(int code, Object[] args) {
		return mondrian.resource.Util.formatError(getError(code), args);
	}
	/**
	 * Retrieves a message by name, and substitutes parameters. If the message
	 * does not exist, returns null.
	 **/
	public String formatError(String name, Object[] args)
	{
		String msg = getError(name);
		if (msg == null) {
			return null;
		}
		return mondrian.resource.Util.formatError(msg, args);
	}
	public String getError(int code) {
		ResourceDef.BaflResourceText res = (ResourceDef.BaflResourceText)
			mapCodeToError.get(new Integer(code));
		if (res == null) {
			return null;
		}
		return res.cdata;
	}
	public String getError(String name) {
		ResourceDef.BaflResourceText res = (ResourceDef.BaflResourceText)
			mapNameToError.get(name);
		if (res == null) {
			return null;
		}
		return res.cdata;
	}

	// implement Resource
	public int getSeverity(int code)
	{
		ResourceDef.BaflResourceText res = (ResourceDef.BaflResourceText)
			mapCodeToError.get(new Integer(code));
		return res.getSeverity();
	}

	public void assert(boolean b)
	{
		if (!b) {
			throw newInternalError("assert failed");
		}
	}
	public void assert(boolean b, String s)
	{
		if (!b) {
			throw newInternalError("assert failed: " + s);
		}
	}
	/**
	 * Throws an internal error. Called from {@link #assert(boolean)}.
	 **/
	public java.lang.Error newInternalError(String s)
	{
		return newInternalError(null, s);
	}
	/**
	 * Throws an internal error. Called from {@link #assert(boolean)}. Derived
	 * classes should implement this method to throw their own particular
	 * 'internal error' message.
	 **/
	public abstract java.lang.Error newInternalError(Throwable err, String s);
}


// End ResourceBase.java
