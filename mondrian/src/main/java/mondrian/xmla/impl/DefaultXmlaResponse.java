/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2028-08-13
 ******************************************************************************/

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
