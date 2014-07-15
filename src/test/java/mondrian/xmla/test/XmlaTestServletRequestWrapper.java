/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2006-2011 Pentaho and others
// All Rights Reserved.
*/
package mondrian.xmla.test;

import mondrian.olap.Util;
import mondrian.xmla.XmlaServlet;

import java.io.*;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

/**
 * Dummy request for testing XmlaServlet. Provides a 'text/xml' content stream
 * from a post from xmlaTest.jsp. Assumes that the SOAPRequest parameter
 * contains XML/A SOAP request body.
 *
 * @author Sherman Wood
 */
public class XmlaTestServletRequestWrapper extends HttpServletRequestWrapper {

    private HttpServletRequest originalRequest;
    private ServletInputStream servletInStream;

    public XmlaTestServletRequestWrapper(HttpServletRequest req) {
        super(req);
        originalRequest = req;
        init();
    }

    /**
     * Extract the data from the HTTP request and create an XML/A request
     */
    private void init() {
        String soapRequest = originalRequest.getParameter("SOAPRequest");

        if (soapRequest == null || soapRequest.length() == 0) {
            // Parameter not set. Look for the request in the body of the http
            // request.

            try {
                final ServletInputStream inputStream =
                        originalRequest.getInputStream();
                soapRequest = Util.readFully(
                    new InputStreamReader(inputStream), 2048);
            } catch (IOException e) {
                throw Util.newInternal(e, "error reading body of soap request");
            }

            if (soapRequest.length() == 0) {
                throw new RuntimeException("SOAPRequest not set");
            }
        }

        /*
         * Strip the XML premable if it is there
         */
        if (soapRequest.indexOf("<?") == 0) {
            soapRequest = soapRequest.substring(soapRequest.indexOf("?>") + 2);
        }

        /*
         * Make a SOAP message
         */
        String request =
            "<?xml version=\"1.0\"?>\r\n"
            + "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\""
            + XmlaServlet.NS_SOAP_ENV_1_1
            + "\" SOAP-ENV:encodingStyle=\""
            + XmlaServlet.NS_SOAP_ENC_1_1
            + "\">\r\n"
            + "<SOAP-ENV:Header/>\r\n"
            + "<SOAP-ENV:Body>\r\n"
            + soapRequest
            + "</SOAP-ENV:Body>\r\n</SOAP-ENV:Envelope>\r\n";

        servletInStream = new XmlaTestServletInputStream(request);
    }

    public String getContentType() {
        return "text/xml";
    }

    public ServletInputStream getInputStream() {
        return servletInStream;
    }

    private static class XmlaTestServletInputStream extends ServletInputStream {

        private ByteArrayInputStream bais;

        XmlaTestServletInputStream(String source) {
            bais = new ByteArrayInputStream(source.getBytes());
        }

        public int readLine(byte[] arg0, int arg1, int arg2)
            throws IOException
        {
            return bais.read(arg0, arg1, arg2);
        }

        public int available() throws IOException {
            return bais.available();
        }

        public void close() throws IOException {
            bais.close();
        }

        public synchronized void mark(int readlimit) {
            bais.mark(readlimit);
        }

        public boolean markSupported() {
            return bais.markSupported();
        }

        public int read() throws IOException {
            return bais.read();
        }

        public int read(byte[] b, int off, int len) throws IOException {
            return bais.read(b, off, len);
        }

        public int read(byte[] b) throws IOException {
            return bais.read(b);
        }

        public synchronized void reset() throws IOException {
             bais.reset();
        }

        public long skip(long n) throws IOException {
            return bais.skip(n);
        }
    }
}

// End XmlaTestServletRequestWrapper.java
