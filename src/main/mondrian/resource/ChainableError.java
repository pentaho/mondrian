/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 3 December, 2001
*/

package mondrian.resource;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.Method;

/**
 * A <code>ChainableError</code> is an an error which can be constructed from a
 * resource and a set of arguments. It implements {@link ChainableThrowable},
 * so can chain it to a previous {@link Throwable}.
 *
 * @see ChainableException
 * @see ChainableRuntimeException
 *
 * @author jhyde
 * @since 3 December, 2001
 * @version $Id$
 **/
public class ChainableError extends Error implements ChainableThrowable
{
	private Throwable cause;
	private ResourceInstance instance;

	public ChainableError(Throwable cause, ResourceInstance instance)
	{
		super(instance.toString());
		this.instance = instance;
		this.cause = cause;
	}
	/**
	 * Implements {@link ChainableThrowable}.
	 * This method was added to {@link Throwable} in JDK 1.4.
	 */
	public Throwable getCause()
	{
		return cause;
	}
	public ResourceInstance getResourceInstance()
	{
		return instance;
	}
//	public String toString() {
//		return Util.toString(this);
//	}

	public void printStackTrace(PrintWriter s) {
		if (s instanceof Util.DummyPrintWriter) {
			// avoid recursive call
			super.printStackTrace(s);
		} else {
			Util.printStackTrace(this, s);
		}
	}

	public void printStackTrace(PrintStream s) {
		if (s instanceof Util.DummyPrintStream) {
			// avoid recursive call
			super.printStackTrace(s);
		} else {
			Util.printStackTrace(this, s);
		}
	}

	public void printStackTrace() {
		printStackTrace(System.err);
	}
}

// End Error.java
