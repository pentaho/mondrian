/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2001-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 18 June, 2001
*/

package mondrian.xom;
import mondrian.resource.ChainableThrowable;

/**
 * XOMException extends Exception and provides detailed error messages for
 * xom-specific exceptions.
 */
public class XOMException extends Exception implements ChainableThrowable {

	private Throwable next;

	/**
	 * Constructs a mining exception with no message.
	 */
	public XOMException()
	{
		super();
		next = null;
	}

	/**
	 * Constructs a mining exception with a detailed message.
	 *
	 *@param s - a detailed message describing the specific error
	 */
	public XOMException(String s)
	{
		super(s);
		next = null;
	}

	/**
	 * Constructs a mining exception based on another exception, so that
	 * the exceptions may be chained.
	 * @param base the exception on which this one is based.
	 * @param s a message for this portion of the exception.
	 */
	public XOMException(Throwable base, String s)
	{
		super(s);
		next = base;
	}	

	/**
	 * Get the chained exception.
	 * @return the exception chained to this exception, if any.
	 */
	public Throwable getNextException()
	{
		return next;
	}	

	/**
	 * Set the chained exception.
	 * @param th the exception to chain onto this exception.
	 */
	public void setNextException(Throwable th)
	{
		next = th;
	}	

	// implement ChainableThrowable
	public Throwable getNextThrowable()
	{
		return next;
	}

	/**
	 * Override getLocalizedMessage to handle chained exceptions.
	 */
	public String getLocalizedMessage()
	{
		if(next == null)
			return super.getMessage();
		else
			return super.getMessage() + ": " + next.getLocalizedMessage();
	}

	/**
	 * Override getMessage to handle chained exceptions.
	 */
	public String getMessage()
	{
		if(next == null)
			return super.getMessage();
		else
			return super.getMessage() + ": " + next.getMessage();
	}

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
	 * Override printStackTrace to include information about the chained
	 * exception.
	 */
	public void printStackTrace()
	{
		printStackTrace(new java.io.PrintWriter(System.err, true));
	}

	/**
	 * Override printStackTrace to include information about the chained
	 * exception.
	 */
	public void printStackTrace(java.io.PrintStream s)
	{
		printStackTrace(new java.io.PrintWriter(s, true));
	}

	/**
	 * Override printStackTrace to include information about the chained
	 * exception.
	 */	
	public void printStackTrace(java.io.PrintWriter s)
	{
		if(next == null)
			super.printStackTrace(s);
		else {
			// Display exceptions depth-first
			next.printStackTrace(s);
			s.println("Thrown by:");
			super.printStackTrace(s);
		}		
	}

	/**
	 * Override toString to handle chained exceptions.
	 */
	public String toString()
	{
		if(next == null)
			return super.toString();
		else
			return super.toString() + ": " + next.toString();
	}
	
}

// End XOMException.java
