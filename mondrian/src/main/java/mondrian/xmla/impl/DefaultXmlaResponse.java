/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.xmla.impl;

import mondrian.olap.Util;
import mondrian.xmla.*;

import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

/**
 * Default implementation of {@link mondrian.xmla.XmlaResponse}.
 *
 * @author Gang Chen
 */
public class DefaultXmlaResponse implements XmlaResponse  {

    // TODO: add a msg to MondrianResource for this.
    private static final String MSG_ENCODING_ERROR = "Encoding unsupported: ";

    private final SaxWriter writer;

    public DefaultXmlaResponse(
        OutputStream outputStream,
        String encoding,
        Enumeration.ResponseMimeType responseMimeType)
    {
        try {
            switch (responseMimeType) {
            case JSON:
                writer = new JsonSaxWriter(outputStream);
                break;
            case SOAP:
            default:
                writer = new DefaultSaxWriter(outputStream, encoding);
                break;
            }
        } catch (UnsupportedEncodingException uee) {
            throw Util.newError(uee, MSG_ENCODING_ERROR + encoding);
        }
    }

    public SaxWriter getWriter() {
        return writer;
    }

    public void error(Throwable t) {
        writer.completeBeforeElement("root");
        @SuppressWarnings({"ThrowableResultOfMethodCallIgnored"})
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
