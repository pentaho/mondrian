/*
// $Id$
// (C) Copyright 2002 Kana Software, Inc.
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 29 March, 2002
*/
package mondrian.test;

import java.util.regex.Pattern;

import junit.framework.TestSuite;

/**
 * A <code>Testable</code> is an object which knows how to add test cases for
 * itself.
 *
 * @author jhyde
 * @since 29 March, 2002
 * @version $Id$
 **/
public interface Testable {
	/**
	 * Adds test cases to a suite. The default implementation calls every
	 * method which starts with 'test'.
	 */
	void addTests(TestSuite suite, Pattern pattern);
}

// End Testable.java
