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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Element;

import jakarta.servlet.ServletConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base XML/A servlet.
 *
 * @author Gang Chen
 * @since December, 2005
 */
public abstract class XmlaServlet
  extends HttpServlet
  implements XmlaConstants {
  public static final String PARAM_DATASOURCES_CONFIG = "DataSourcesConfig";
  public static final String PARAM_OPTIONAL_DATASOURCE_CONFIG =
    "OptionalDataSourceConfig";
  public static final String PARAM_CHAR_ENCODING = "CharacterEncoding";
  public static final String PARAM_CALLBACKS = "Callbacks";
  protected static final Logger LOGGER = LogManager.getLogger( XmlaServlet.class );
  private final List<XmlaRequestCallback> callbackList =
    new ArrayList<XmlaRequestCallback>();
  protected XmlaHandler xmlaHandler = null;
  protected String charEncoding = null;
  private XmlaHandler.ConnectionFactory connectionFactory;

  public XmlaServlet() {
  }

  /**
   * Returns true if paramName's value is not null and 'true'.
   */
  public static boolean getBooleanInitParameter(
    ServletConfig servletConfig,
    String paramName ) {
    String paramValue = servletConfig.getInitParameter( paramName );
    return paramValue != null && Boolean.valueOf( paramValue );
  }

  public static boolean getParameter(
    HttpServletRequest req,
    String paramName ) {
    String paramValue = req.getParameter( paramName );
    return paramValue != null && Boolean.valueOf( paramValue );
  }

  /**
   * Initializes servlet and XML/A handler.
   */
  public void init( ServletConfig servletConfig )
    throws ServletException {
    super.init( servletConfig );

    // init: charEncoding
    initCharEncodingHandler( servletConfig );

    // init: callbacks
    initCallbacks( servletConfig );

    this.connectionFactory = createConnectionFactory( servletConfig );
  }

  protected abstract XmlaHandler.ConnectionFactory createConnectionFactory(
    ServletConfig servletConfig )
    throws ServletException;

  /**
   * Gets (creating if needed) the XmlaHandler.
   *
   * @return XMLA handler
   */
  protected XmlaHandler getXmlaHandler() {
    if ( this.xmlaHandler == null ) {
      this.xmlaHandler = new XmlaHandler( connectionFactory );
    }
    return this.xmlaHandler;
  }

  /**
   * Registers a callback.
   */
  protected final void addCallback( XmlaRequestCallback callback ) {
    callbackList.add( callback );
  }

  /**
   * Returns the list of callbacks. The list is immutable.
   *
   * @return list of callbacks
   */
  protected final List<XmlaRequestCallback> getCallbacks() {
    return Collections.unmodifiableList( callbackList );
  }

  /**
   * Main entry for HTTP post method
   */
  protected void doPost(
    HttpServletRequest request,
    HttpServletResponse response )
    throws ServletException, IOException {
    // Request Soap Header and Body
    // header [0] and body [1]
    Element[] requestSoapParts = new Element[ 2 ];

    // Response Soap Header and Body
    // An array allows response parts to be passed into callback
    // and possible modifications returned.
    // response header in [0] and response body in [1]
    byte[][] responseSoapParts = new byte[ 2 ][];

    Phase phase = Phase.VALIDATE_HTTP_HEAD;
    Enumeration.ResponseMimeType mimeType =
      Enumeration.ResponseMimeType.SOAP;

    try {
      if ( charEncoding != null ) {
        try {
          request.setCharacterEncoding( charEncoding );
          response.setCharacterEncoding( charEncoding );
        } catch ( UnsupportedEncodingException uee ) {
          charEncoding = null;
          LOGGER.warn(
            "Unsupported character encoding '" + charEncoding
              + "': Use default character encoding from HTTP client "
              + "for now" );
        }
      }

      response.setContentType( mimeType.getMimeType() );

      Map<String, Object> context = new HashMap<String, Object>();

      try {
        if ( LOGGER.isDebugEnabled() ) {
          LOGGER.debug( "Invoking validate http header callbacks" );
        }
        for ( XmlaRequestCallback callback : getCallbacks() ) {
          if ( !callback.processHttpHeader(
            request,
            response,
            context ) ) {
            return;
          }
        }
      } catch ( XmlaException xex ) {
        LOGGER.error(
          "Errors when invoking callbacks validateHttpHeader", xex );
        handleFault( response, responseSoapParts, phase, xex );
        phase = Phase.SEND_ERROR;
        marshallSoapMessage( response, responseSoapParts, mimeType );
        return;
      } catch ( Exception ex ) {
        LOGGER.error(
          "Errors when invoking callbacks validateHttpHeader", ex );
        handleFault(
          response, responseSoapParts,
          phase,
          new XmlaException(
            SERVER_FAULT_FC,
            CHH_CODE,
            CHH_FAULT_FS,
            ex ) );
        phase = Phase.SEND_ERROR;
        marshallSoapMessage( response, responseSoapParts, mimeType );
        return;
      }


      phase = Phase.INITIAL_PARSE;

      try {
        if ( LOGGER.isDebugEnabled() ) {
          LOGGER.debug( "Unmarshalling SOAP message" );
        }

        // check request's content type
        String contentType = request.getContentType();
        if ( contentType == null
          || !contentType.contains( "text/xml" ) ) {
          throw new IllegalArgumentException(
            "Only accepts content type 'text/xml', not '"
              + contentType + "'" );
        }

        // are they asking for a JSON response?
        String accept = request.getHeader( "Accept" );
        if ( accept != null ) {
          mimeType = XmlaUtil.chooseResponseMimeType( accept );
          if ( mimeType == null ) {
            throw new IllegalArgumentException(
              "Accept header '" + accept + "' is not a supported"
                + " response content type. Allowed values:"
                + " text/xml, application/xml, application/json." );
          }
          if ( mimeType != Enumeration.ResponseMimeType.SOAP ) {
            response.setContentType( mimeType.getMimeType() );
          }
        }
        context.put( CONTEXT_MIME_TYPE, mimeType );

        unmarshallSoapMessage( request, requestSoapParts );
      } catch ( XmlaException xex ) {
        LOGGER.error( "Unable to unmarshall SOAP message", xex );
        handleFault( response, responseSoapParts, phase, xex );
        phase = Phase.SEND_ERROR;
        marshallSoapMessage( response, responseSoapParts, mimeType );
        return;
      }

      phase = Phase.PROCESS_HEADER;

      try {
        if ( LOGGER.isDebugEnabled() ) {
          LOGGER.debug( "Handling XML/A message header" );
        }

        // process application specified SOAP header here
        handleSoapHeader(
          response,
          requestSoapParts,
          responseSoapParts,
          context );
      } catch ( XmlaException xex ) {
        LOGGER.error( "Errors when handling XML/A message", xex );
        handleFault( response, responseSoapParts, phase, xex );
        phase = Phase.SEND_ERROR;
        marshallSoapMessage( response, responseSoapParts, mimeType );
        return;
      }

      phase = Phase.CALLBACK_PRE_ACTION;


      try {
        if ( LOGGER.isDebugEnabled() ) {
          LOGGER.debug( "Invoking callbacks preAction" );
        }

        for ( XmlaRequestCallback callback : getCallbacks() ) {
          callback.preAction( request, requestSoapParts, context );
        }
      } catch ( XmlaException xex ) {
        LOGGER.error( "Errors when invoking callbacks preaction", xex );
        handleFault( response, responseSoapParts, phase, xex );
        phase = Phase.SEND_ERROR;
        marshallSoapMessage( response, responseSoapParts, mimeType );
        return;
      } catch ( Exception ex ) {
        LOGGER.error( "Errors when invoking callbacks preaction", ex );
        handleFault(
          response, responseSoapParts,
          phase,
          new XmlaException(
            SERVER_FAULT_FC,
            CPREA_CODE,
            CPREA_FAULT_FS,
            ex ) );
        phase = Phase.SEND_ERROR;
        marshallSoapMessage( response, responseSoapParts, mimeType );
        return;
      }

      phase = Phase.PROCESS_BODY;

      try {
        if ( LOGGER.isDebugEnabled() ) {
          LOGGER.debug( "Handling XML/A message body" );
        }

        // process XML/A request
        handleSoapBody(
          response,
          requestSoapParts,
          responseSoapParts,
          context );
      } catch ( XmlaException xex ) {
        LOGGER.error( "Errors when handling XML/A message", xex );
        handleFault( response, responseSoapParts, phase, xex );
        phase = Phase.SEND_ERROR;
        marshallSoapMessage( response, responseSoapParts, mimeType );
        return;
      }

      mimeType =
        (Enumeration.ResponseMimeType) context.get( CONTEXT_MIME_TYPE );

      phase = Phase.CALLBACK_POST_ACTION;

      try {
        if ( LOGGER.isDebugEnabled() ) {
          LOGGER.debug( "Invoking callbacks postAction" );
        }

        for ( XmlaRequestCallback callback : getCallbacks() ) {
          callback.postAction(
            request, response,
            responseSoapParts, context );
        }
      } catch ( XmlaException xex ) {
        LOGGER.error( "Errors when invoking callbacks postaction", xex );
        handleFault( response, responseSoapParts, phase, xex );
        phase = Phase.SEND_ERROR;
        marshallSoapMessage( response, responseSoapParts, mimeType );
        return;
      } catch ( Exception ex ) {
        LOGGER.error( "Errors when invoking callbacks postaction", ex );
        handleFault(
          response,
          responseSoapParts,
          phase,
          new XmlaException(
            SERVER_FAULT_FC,
            CPOSTA_CODE,
            CPOSTA_FAULT_FS,
            ex ) );
        phase = Phase.SEND_ERROR;
        marshallSoapMessage( response, responseSoapParts, mimeType );
        return;
      }

      phase = Phase.SEND_RESPONSE;

      try {
        response.setStatus( HttpServletResponse.SC_OK );
        marshallSoapMessage( response, responseSoapParts, mimeType );
      } catch ( XmlaException xex ) {
        LOGGER.error( "Errors when handling XML/A message", xex );
        handleFault( response, responseSoapParts, phase, xex );
        phase = Phase.SEND_ERROR;
        marshallSoapMessage( response, responseSoapParts, mimeType );
      }
    } catch ( Throwable t ) {
      LOGGER.error( "Unknown Error when handling XML/A message", t );
      handleFault( response, responseSoapParts, phase, t );
      marshallSoapMessage( response, responseSoapParts, mimeType );
    }
  }

  /**
   * Implement to provide application specified SOAP unmarshalling algorithm.
   */
  protected abstract void unmarshallSoapMessage(
    HttpServletRequest request,
    Element[] requestSoapParts )
    throws XmlaException;

  /**
   * Implement to handle application specified SOAP header.
   */
  protected abstract void handleSoapHeader(
    HttpServletResponse response,
    Element[] requestSoapParts,
    byte[][] responseSoapParts,
    Map<String, Object> context )
    throws XmlaException;

  /**
   * Implement to handle XML/A request.
   */
  protected abstract void handleSoapBody(
    HttpServletResponse response,
    Element[] requestSoapParts,
    byte[][] responseSoapParts,
    Map<String, Object> context )
    throws XmlaException;

  /**
   * Implement to provide application specified SOAP marshalling algorithm.
   */
  protected abstract void marshallSoapMessage(
    HttpServletResponse response,
    byte[][] responseSoapParts,
    Enumeration.ResponseMimeType responseMimeType )
    throws XmlaException;

  /**
   * Implement to application specified handler of SOAP fualt.
   */
  protected abstract void handleFault(
    HttpServletResponse response,
    byte[][] responseSoapParts,
    Phase phase,
    Throwable t );

  /**
   * Initialize character encoding
   */
  protected void initCharEncodingHandler( ServletConfig servletConfig ) {
    String paramValue = servletConfig.getInitParameter( PARAM_CHAR_ENCODING );
    if ( paramValue != null ) {
      this.charEncoding = paramValue;
    } else {
      this.charEncoding = null;
      LOGGER.warn( "Use default character encoding from HTTP client" );
    }
  }

  /**
   * Registers callbacks configured in web.xml.
   */
  protected void initCallbacks( ServletConfig servletConfig ) {
    String callbacksValue = servletConfig.getInitParameter( PARAM_CALLBACKS );

    if ( callbacksValue != null ) {
      String[] classNames = callbacksValue.split( ";" );

      int count = 0;
      nextCallback:
      for ( String className1 : classNames ) {
        String className = className1.trim();

        try {
          Class<?> cls = Class.forName( className );
          if ( XmlaRequestCallback.class.isAssignableFrom( cls ) ) {
            XmlaRequestCallback callback =
              (XmlaRequestCallback) cls.newInstance();

            try {
              callback.init( servletConfig );
            } catch ( Exception e ) {
              LOGGER.warn(
                "Failed to initialize callback '"
                  + className + "'",
                e );
              continue nextCallback;
            }

            addCallback( callback );
            count++;

            if ( LOGGER.isDebugEnabled() ) {
              LOGGER.debug(
                "Register callback '" + className + "'" );
            }
          } else {
            LOGGER.warn(
              "'" + className + "' is not an implementation of '"
                + XmlaRequestCallback.class + "'" );
          }
        } catch ( ClassNotFoundException cnfe ) {
          LOGGER.warn(
            "Callback class '" + className + "' not found",
            cnfe );
        } catch ( InstantiationException ie ) {
          LOGGER.warn(
            "Can't instantiate class '" + className + "'",
            ie );
        } catch ( IllegalAccessException iae ) {
          LOGGER.warn(
            "Can't instantiate class '" + className + "'",
            iae );
        }
      }
      LOGGER.debug(
        "Registered " + count + " callback" + ( count > 1 ? "s" : "" ) );
    }
  }

  public enum Phase {
    VALIDATE_HTTP_HEAD,
    INITIAL_PARSE,
    CALLBACK_PRE_ACTION,
    PROCESS_HEADER,
    PROCESS_BODY,
    CALLBACK_POST_ACTION,
    SEND_RESPONSE,
    SEND_ERROR
  }
}
