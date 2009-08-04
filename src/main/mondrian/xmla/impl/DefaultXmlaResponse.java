/*
// $Id$
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// Copyright (C) 2005-2007 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla.impl;

import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

import mondrian.olap.Util;
import mondrian.xmla.*;

/**
 * Default implementation of {@link mondrian.xmla.XmlaResponse}.
 *
 * @author Gang Chen
 */
public class DefaultXmlaResponse implements XmlaResponse  {

    // TODO: add a msg to MondrianResource for this.
    private static final String MSG_ENCODING_ERROR = "Encoding unsupported: ";

    private final SaxWriter writer;

    public DefaultXmlaResponse(OutputStream outputStream, String encoding) {
        try {
            writer = new DefaultSaxWriter(outputStream, encoding);
        } catch (UnsupportedEncodingException uee) {
            throw Util.newError(uee, MSG_ENCODING_ERROR + encoding);
        }
    }

    public SaxWriter getWriter() {
        return writer;
    }

    public void error(Throwable t) {
        writer.completeBeforeElement("root");
        Throwable throwable = XmlaUtil.rootThrowable(t);
        writer.startElement("Messages");
        writer.startElement(
            "Error",
            "ErrorCode", throwable.getClass().getName(),
            "Description", throwable.getMessage(),
            "Source", "Mondrian",
            "Help", "");
        writer.endElement(); // </Messages>
        writer.endElement(); // </Error>
    }

}

// End DefaultXmlaResponse.java
