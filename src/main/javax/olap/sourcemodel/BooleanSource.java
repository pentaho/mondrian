/*
 * Java(TM) OLAP Interface
 */
package javax.olap.sourcemodel;



public interface BooleanSource
extends javax.olap.sourcemodel.Source {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.sourcemodel.BooleanSource and( boolean rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource and( javax.olap.sourcemodel.BooleanSource rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource appendValue( boolean appendValue )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource appendValues( boolean[] appendValues )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource eq( boolean rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource forAll()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource forAll( boolean noValueAsFalse )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource forAny()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource forAny( boolean noValueAsFalse )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource forNone()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource forNone( boolean noValueAsFalse )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource ge( boolean rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource gt( boolean rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource implies( java.util.Date ifTrue, java.util.Date ifFalse )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.DateSource implies( javax.olap.sourcemodel.DateSource ifTrue, javax.olap.sourcemodel.DateSource ifFalse )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource implies( double ifTrue, double ifFalse )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource implies( int ifTrue, int ifFalse )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource implies( javax.olap.sourcemodel.NumberSource ifTrue, javax.olap.sourcemodel.NumberSource ifFalse )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource implies( javax.olap.sourcemodel.StringSource ifTrue, javax.olap.sourcemodel.StringSource ifFalse )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource implies( java.lang.String ifTrue, java.lang.String ifFalse )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource le( boolean rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource lt( boolean rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource ne( boolean rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource not()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource or( boolean rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource positionOfValue( boolean value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource positionOfValues( boolean[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource removeValue( boolean value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource removeValues( boolean[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource selectValue( boolean value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource selectValues( boolean[] values )
    throws javax.olap.OLAPException;

}
