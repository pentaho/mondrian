/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2004-2005 TONBELLER AG
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.xmla;

import mondrian.olap.MondrianException;

/** 
 * An exception thrown while processing an XMLA request. The faultcode
 * corresponds to the SOAP Fault faultcode and the faultstring
 * to the SOAP Fault faultstring.
 * 
 * @author <a>Richard M. Emberson</a>
 * @version $Id$
 */
public class XmlaException extends MondrianException {

    public static String formatFaultCode(XmlaException xex) {
        return formatFaultCode(xex.getFaultCode(), xex.getCode());
    }
    public static String formatFaultCode(String faultCode, String code) {
        return formatFaultCode(XmlaConstants.SOAP_PREFIX, 
                faultCode, code);
    }
    public static String formatFaultCode(String nsPrefix, 
                String faultCode, String code) {
        return nsPrefix + 
            ':' + 
            faultCode +
            '.' + 
            code;
    }
    public static String formatDetail(String msg) {
        return XmlaConstants.FAULT_FS_PREFIX + msg;
    }

    public static Throwable getRootCause(Throwable throwable) {
        Throwable t = throwable;
        while (t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }

    private final String faultCode; 
    private final String code; 
    private final String faultString; 

    public XmlaException(
            String faultCode, 
            String code, 
            String faultString,
            Throwable cause) {
        super(faultString, cause);
        this.faultCode = faultCode;
        this.code = code;
        this.faultString = faultString;
    }

    public String getFaultCode() {
        return faultCode;
    }
    public String getCode() {
        return code;
    }
    public String getFaultString() {
        return faultString;
    }
    public String getDetail() {
        Throwable t = getCause();
        t = getRootCause(t);
        String detail = t.getMessage();
        return (detail != null)
            ? detail
            : t.getClass().getName();
    }
}

