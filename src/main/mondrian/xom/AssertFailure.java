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

package mondrian.xom;

/**
 * todo:
 *
 * @author jhyde
 * @since 3 December, 2001
 * @version $Id$
 **/
public class AssertFailure extends RuntimeException {
    /** Construct an AssertFailure with no message */
    public AssertFailure() {
		super();
    }

    /** Construct an AssertFailure with a simple detail message. */
    public AssertFailure(String s) {
		super(s);
    }

    /** Construct an AssertFailure from an exception.  This indicates an
	 * unexpected exception of another type.  We'll fill in the stack trace
	 * when printing the message. */
    public AssertFailure(Throwable th) {
		super("unexpected exception:\n" +
			  th.fillInStackTrace().toString());
    }

    /** Similar to the previous constructor, except allows a custom message on
	 * top of the exception */
    public AssertFailure(Throwable th, String s) {
		super(s + ":\n" +
			  th.fillInStackTrace().toString());
    }
}

// End AssertFailure.java
