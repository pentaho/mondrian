/*
 * Java(TM) OLAP Interface
 */
package javax.olap.sourcemodel;



public interface NumberSource
extends javax.olap.sourcemodel.Source {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.sourcemodel.NumberSource abs()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource appendValue( double appendValue )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource appendValue( float appendValue )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource appendValue( int appendValue )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource appendValue( short appendValue )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource appendValues( double[] appendValues )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource appendValues( float[] appendValues )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource appendValues( int[] appendValues )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource appendValues( short[] appendValues )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource arccos()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource arcsin()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource arctan()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource average()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource average( boolean noValueAsZero )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource cos()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource cosh()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource div( double rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource div( float rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource div( int rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource div( javax.olap.sourcemodel.NumberSource rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource div( short rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource eq( double rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource eq( float rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource eq( int rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource eq( short rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource ge( double rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource ge( float rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource ge( int rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource ge( short rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource gt( double rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource gt( float rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource gt( int rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource gt( short rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource intpart()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource le( double rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource le( float rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource le( int rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource le( short rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource log()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource lt( double rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource lt( float rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource lt( int rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource lt( short rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource maximum()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource maximum( boolean noValueAsZero )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource minimum()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource minimum( boolean noValueAsZero )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource minus( double rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource minus( float rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource minus( int rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource minus( short rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource minus( javax.olap.sourcemodel.NumberSource rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource ne( double rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource ne( float rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource ne( int rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource ne( short rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource negate()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource plus( double rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource plus( float rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource plus( int rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource plus( javax.olap.sourcemodel.NumberSource rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource plus( short rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource positionOfValue( double value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource positionOfValue( float value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource positionOfValue( int value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource positionOfValue( short value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource positionOfValues( double[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource positionOfValues( float[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource positionOfValues( int[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource positionOfValues( short[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource pow( double rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource pow( float rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource pow( int rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource pow( javax.olap.sourcemodel.NumberSource rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource pow( short rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource rem( double rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource rem( float rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource rem( int rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource rem( javax.olap.sourcemodel.NumberSource rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource rem( short rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource removeValue( double value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource removeValue( float value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource removeValue( int value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource removeValue( short value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource removeValues( double[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource removeValues( float[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource removeValues( int[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource removeValues( short[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource round( double multiple )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource round( float multiple )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource round( int multiple )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource round( javax.olap.sourcemodel.NumberSource multiple )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource round( short multiple )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource selectValue( double value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource selectValue( float value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource selectValue( int value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource selectValue( short value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource selectValues( double[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource selectValues( float[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource selectValues( int[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource selectValues( short[] value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource sin()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource sinh()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource sqrt()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource stdev()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource stdev( boolean noValueAsZero )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource tan()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource tanh()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource times( double rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource times( float rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource times( int rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource times( javax.olap.sourcemodel.NumberSource rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource times( short rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource total()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource total( boolean noValueAsZero )
    throws javax.olap.OLAPException;

}
