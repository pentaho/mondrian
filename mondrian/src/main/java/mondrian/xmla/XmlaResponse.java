/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


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
