/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2006-2006 Julian Hyde
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

/**
 * ...
 *
 * @author jhyde
 * @version $Id$
 * @since Apr 12, 2006
 */
public class XmlaException extends RuntimeException {
    private final String code;
    private final String faultString;

    public XmlaException(String message,Throwable cause) {
        this(null, null, message, cause);
    }

    public XmlaException(String faultString, String code, String message,Throwable cause) {
        super(message, cause);
        this.code = code;
        this.faultString = faultString;
    }

    public String getCode() {
        return code;
    }

    public String getFaultString() {
        return faultString;
    }

    public static String formatFaultCode(XmlaException xex) {
        return xex.faultString;
    }

    public static String formatDetail(Object detail) {
        throw new UnsupportedOperationException();
    }

    public Object getDetail() {
        throw new UnsupportedOperationException();
    }

    public static Throwable getRootCause(Throwable t) {
        throw new UnsupportedOperationException();
    }

    public static String formatFaultCode(String serverFaultFc, String code) {
        throw new UnsupportedOperationException();
    }
}

// End XmlaException.java
