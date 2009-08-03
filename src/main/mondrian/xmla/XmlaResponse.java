/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2005-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;


/**
 * XML/A response interface.
 *
 * @author Gang Chen
 */
public interface XmlaResponse {

    /**
     * Report XML/A error (not SOAP fault).
     */
    public void error(Throwable t);

    /**
     * Get helper for writing XML document.
     */
    public SaxWriter getWriter();
}

// End XmlaResponse.java
