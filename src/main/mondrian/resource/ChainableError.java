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

/**
 * An <code>Error</code> is an an error which can be constructed from a
 * resource and a set of arguments. It implements {@link ChainableThrowable},
 * so can chain it to a previous error.
 *
 * @author jhyde
 * @since 3 December, 2001
 * @version $Id$
 **/
public class Error extends java.lang.Error implements ChainableThrowable
{
	private Throwable nextThrowable;
	private int code;

	public Error(Throwable err, Resource res, int code, Object[] args)
	{
		super(res.formatError(code, args));
		this.nextThrowable = err;
		this.code = code;
	}
	// implement ChainableThrowable
	public Throwable getNextThrowable()
	{
		return nextThrowable;
	}
	public void setNextThrowable(Throwable err)
  	{
		nextThrowable = err;
	}
	public int getCode()
	{
		return code;
	}
//	public String toString() {
//		return Util.toString(this);
//	}
	public void printStackTrace(PrintWriter s) {
		super.printStackTrace(s);
		if (nextThrowable != null) {
			nextThrowable.printStackTrace(s);
		}
	}
	public void printStackTrace(PrintStream s) {
		super.printStackTrace(s);
		if (nextThrowable != null) {
			nextThrowable.printStackTrace(s);
		}
	}
}


// End Error.java
