/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// (C) Copyright 1998-2002 Kana Software, Inc. and others.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, 20 January, 1999
*/

package mondrian.olap;
import java.util.*;

/**
 * Lexical analyzer whose input is a string.
 */
public class StringScanner extends Scanner {
	private String s;
	private int i;

	public StringScanner(String s, boolean debug)
	{
		super(debug);
		this.s = s;
		i = 0;
	}
	
	// Override Scanner.getChar().
	protected int getChar()
	{
		if (i >= s.length())
			return -1;
		else
			return s.charAt(i++);
	}
}

// End StringScanner.java
