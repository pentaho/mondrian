/*
 * Java(TM) OLAP Interface
 */
package javax.olap.sourcemodel;



public interface DateSource
extends javax.olap.sourcemodel.Source {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.sourcemodel.DateSource appendValue( java.util.Date appendValue )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource appendValues( java.util.Date[] appendValues )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource eq( java.util.Date rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource ge( java.util.Date rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource gt( java.util.Date rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource le( java.util.Date rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource lt( java.util.Date rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource ne( java.util.Date rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource plusDays( int rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource plusDays( javax.olap.sourcemodel.NumberSource rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource plusMonths( int rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource plusMonths( javax.olap.sourcemodel.NumberSource rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource positionOfValue( java.util.Date value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource positionOfValues( java.util.Date[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource removeValue( java.util.Date value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource removeValues( java.util.Date[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource selectValue( java.util.Date value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource selectValues( java.util.Date[] values )
    throws javax.olap.OLAPException;

}
