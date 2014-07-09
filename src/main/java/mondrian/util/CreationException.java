/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2007 Pentaho and others
// All Rights Reserved.
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
