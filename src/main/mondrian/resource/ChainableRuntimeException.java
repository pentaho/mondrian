/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 19 September, 2002
*/
package mondrian.resource;

import java.io.PrintWriter;
import java.io.PrintStream;

/**
 * A <code>ChainableRuntimeException</code> is a {@link RuntimeException}
 * which implements {@link ChainableThrowable},
 * so can chain it to a previous {@link Throwable}.
 *
 * @see ChainableError
 * @see ChainableRuntimeException
 *
 * @author jhyde
 * @since 19 September, 2002
 * @version $Id$
 **/
public class ChainableRuntimeException
		extends RuntimeException implements ChainableThrowable {
	private Throwable cause;
	private ResourceInstance instance;

	public ChainableRuntimeException(String message, Throwable cause) {
		super(message);
		this.instance = null;
		this.cause = cause;
	}
	public ChainableRuntimeException(ResourceInstance instance, Throwable cause) {
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
	/**
	 * This method was added to {@link Throwable} in JDK 1.4.
	 */
	public void initCause(Throwable cause)
  	{
		this.cause = cause;
	}
	/**
	 * Override getLocalizedMessage to handle chained exceptions.
	 o/
	public String getLocalizedMessage()
	{
		if(cause == null)
			return super.getMessage();
		else
			return super.getMessage() + ": " + cause.getLocalizedMessage();
	}

	/**
	 * Override getMessage to handle chained exceptions.
	 o/
	public String getMessage()
	{
		if(cause == null)
			return super.getMessage();
		else
			return super.getMessage() + ": " + cause.getMessage();
	}
*/

	/**
	 * Return only the first message of this exception.  Ignore all
	 * chained exceptions.
	 * @return the first error message in this exception.
	 */
	public String getFirstMessage()
	{
		return super.getMessage();
	}

	/**
	 * Override toString to handle chained exceptions.
	 o/
	public String toString() {
		return Util.toString(this);
	}
	*/

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

// End ChainableRuntimeException.java
