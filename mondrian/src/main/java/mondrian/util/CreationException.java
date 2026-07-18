/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 - 2026 by Pentaho Canada Inc. : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2030-06-15
 ******************************************************************************/



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
