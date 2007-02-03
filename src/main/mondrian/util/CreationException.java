/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
*/
package mondrian.util;

public class CreationException extends RuntimeException {
    public CreationException() {
    }
    public CreationException(String s) {
        super(s);
    }
    public CreationException(String s, Throwable t) {
        super(s, t);
    }
    public CreationException(Throwable t) {
        super(t);
    }
}

