/*
 * Java(TM) OLAP Interface
 */
package javax.olap.sourcemodel;



public interface StringSource
extends javax.olap.sourcemodel.Source {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.sourcemodel.StringSource appendValue( java.lang.String appendValue )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource appendValues( java.lang.String[] appendValues )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource eq( java.lang.String rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource ge( java.lang.String rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource gt( java.lang.String rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource indexOf( java.lang.String substring, int fromIndex )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource indexOf( javax.olap.sourcemodel.StringSource substring, javax.olap.sourcemodel.NumberSource fromIndex )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource le( java.lang.String rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource length()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource like( java.lang.String rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource like( javax.olap.sourcemodel.StringSource rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource lt( java.lang.String rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource ne( java.lang.String rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource positionOfValue( java.lang.String value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource positionOfValues( java.lang.String[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource remove( int index, int length )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource remove( javax.olap.sourcemodel.NumberSource index, javax.olap.sourcemodel.NumberSource length )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource removeValue( java.lang.String value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource removeValues( java.lang.String[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource replace( javax.olap.sourcemodel.StringSource oldString, javax.olap.sourcemodel.StringSource newString )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource replace( java.lang.String oldString, java.lang.String newString )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource selectValue( java.lang.String value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource selectValues( java.lang.String[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource substring( int index, int length )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource substring( javax.olap.sourcemodel.NumberSource index, javax.olap.sourcemodel.NumberSource length )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource textFill( int width )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource textFill( javax.olap.sourcemodel.NumberSource width )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource toLowercase()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource toUppercase()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource trim()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource trimLeading()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource trimTrailing()
    throws javax.olap.OLAPException;

}
