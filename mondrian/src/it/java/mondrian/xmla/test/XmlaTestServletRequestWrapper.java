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


package mondrian.xmla.test;

import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import mondrian.olap.Util;
import mondrian.xmla.XmlaServlet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

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

  public XmlaTestServletRequestWrapper( HttpServletRequest req ) {
    super( req );
    originalRequest = req;
    init();
  }

  /**
   * Extract the data from the HTTP request and create an XML/A request
   */
  private void init() {
    String soapRequest = originalRequest.getParameter( "SOAPRequest" );

    if ( soapRequest == null || soapRequest.isEmpty() ) {
      // Parameter not set. Look for the request in the body of the http request.

      try {
        final ServletInputStream inputStream = originalRequest.getInputStream();
        soapRequest = Util.readFully( new InputStreamReader( inputStream ), 2048 );
      } catch ( IOException e ) {
        throw Util.newInternal( e, "error reading body of soap request" );
      }

      if ( soapRequest.isEmpty() ) {
        throw new RuntimeException( "SOAPRequest not set" );
      }
    }

    /*
     * Strip the XML preamble if it is there
     */
    if ( soapRequest.indexOf( "<?" ) == 0 ) {
      soapRequest = soapRequest.substring( soapRequest.indexOf( "?>" ) + 2 );
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

    servletInStream = new XmlaTestServletInputStream( request );
  }

  @Override
  public String getContentType() {
    return "text/xml";
  }

  @Override
  public ServletInputStream getInputStream() {
    return servletInStream;
  }

  private static class XmlaTestServletInputStream extends ServletInputStream {

    private ByteArrayInputStream bais;

    XmlaTestServletInputStream( String source ) {
      bais = new ByteArrayInputStream( source.getBytes() );
    }

    @Override
    public int readLine( byte[] arg0, int arg1, int arg2 ) throws IOException {
      return bais.read( arg0, arg1, arg2 );
    }

    @Override
    public boolean isFinished() {
      return false;
    }

    @Override
    public boolean isReady() {
      return false;
    }

    @Override
    public void setReadListener( ReadListener readListener ) {

    }

    @Override
    public int available() throws IOException {
      return bais.available();
    }

    @Override
    public void close() throws IOException {
      bais.close();
    }

    @Override
    public synchronized void mark( int readLimit ) {
      bais.mark( readLimit );
    }

    @Override
    public boolean markSupported() {
      return bais.markSupported();
    }

    public int read() throws IOException {
      return bais.read();
    }

    @Override
    public int read( byte[] b, int off, int len ) throws IOException {
      return bais.read( b, off, len );
    }

    @Override
    public int read( byte[] b ) throws IOException {
      return bais.read( b );
    }

    @Override
    public synchronized void reset() throws IOException {
      bais.reset();
    }

    @Override
    public long skip( long n ) throws IOException {
      return bais.skip( n );
    }
  }
}

// End XmlaTestServletRequestWrapper.java
