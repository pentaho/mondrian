package mondrian.xmla.test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import mondrian.xmla.XmlaServlet;

/**
 * Dummy request for testing XmlaServlet. Provides a 'text/xml' content
 * stream from a post from xmlaTest.jsp. Assumes that the SOAPRequest parameter
 * contains XML/A SOAP request body
 *  
 * @author Sherman Wood
 * @version $Id$
 *
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
			throw new RuntimeException("SOAPRequest not set");
		}
        
        /*
         * Strip the XML premable if it is there
         */
        if (soapRequest.indexOf("<?") == 0 ) {
        	soapRequest = soapRequest.substring(soapRequest.indexOf("?>") + 2);
        }
		
		/*
		 * Make a SOAP message
		 */
        StringBuffer buf = new StringBuffer();
        buf.append("<?xml version=\"1.0\"?>\r\n");
        buf.append("<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"").append(XmlaServlet.NS_SOAP).
            append("\" SOAP-ENV:encodingStyle=\"").
            append(XmlaServlet.NS_SOAP_ENCODING_STYLE).append("\">\r\n");
        buf.append("<SOAP-ENV:Header/>\r\n");
        buf.append("<SOAP-ENV:Body>\r\n");
        buf.append(soapRequest);
        buf.append("</SOAP-ENV:Body>\r\n</SOAP-ENV:Envelope>\r\n");
		
		servletInStream = new XmlaTestServletInputStream(buf.toString());
	}
	
	public String getContentType() {
		return "text/xml";
	}
	
	public ServletInputStream getInputStream() {
		return servletInStream;
	}
	
	private class XmlaTestServletInputStream extends ServletInputStream {
		
		private ByteArrayInputStream bios;
		
		XmlaTestServletInputStream(String source) {
			bios = new ByteArrayInputStream(source.getBytes());
		}

		public int readLine(byte[] arg0, int arg1, int arg2) throws IOException {
			return bios.read(arg0, arg1, arg2);
		}

		public int available() throws IOException {
			return bios.available();
		}

		public void close() throws IOException {
			bios.close();
		}

		public synchronized void mark(int readlimit) {
			bios.mark(readlimit);
		}

		public boolean markSupported() {
			return bios.markSupported();
		}

		public int read() throws IOException {
			return bios.read();
		}

		public int read(byte[] b, int off, int len) throws IOException {
			return bios.read(b, off, len);
		}

		public int read(byte[] b) throws IOException {
			return bios.read(b);
		}

		public synchronized void reset() throws IOException {
			 bios.reset();
		}

		public long skip(long n) throws IOException {
			return bios.skip(n);
		}
	}
}
