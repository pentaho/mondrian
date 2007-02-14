/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2007-2007 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.util;

import mondrian.olap.MondrianException;

public class CreationException extends MondrianException {
    public CreationException() {
        super();
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


// End CreationException.java
