/*
 * Java(TM) OLAP Interface
 */
package javax.olap.sourcemodel;



public interface Source {

	// ------------------------------------------------
	// -----   Reference-Generated                -----
	// ------------------------------------------------

  public java.util.Collection getSourceOutput()
    throws javax.olap.OLAPException;

  public java.util.Collection getSourceInput()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source getType()
    throws javax.olap.OLAPException;

  public void setType( javax.olap.sourcemodel.Source value )
    throws javax.olap.OLAPException;

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.sourcemodel.Source alias()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source appendValue( javax.olap.sourcemodel.Source appendValue )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source appendValues( javax.olap.sourcemodel.Source appendValues )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source appendValues( javax.olap.sourcemodel.Source[] appendValues )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source at( int position )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source at( javax.olap.sourcemodel.NumberSource position )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource count()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource count( boolean includeNoValue )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source cumulativeInterval()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source cumulativeInterval( int offset )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source cumulativeInterval( javax.olap.sourcemodel.Source offset )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source distinct()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source eq( javax.olap.sourcemodel.Source rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source extract()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source findMatchFor( javax.olap.sourcemodel.Source input )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source first()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource ge( javax.olap.sourcemodel.Source rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source getDataType()
    throws javax.olap.OLAPException;

  public java.util.Set getInputs()
    throws javax.olap.OLAPException;

  public java.util.List getOutputs()
    throws javax.olap.OLAPException;

  public void gt( javax.olap.sourcemodel.Source rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource hasValue()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource in( javax.olap.sourcemodel.Source list )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source interval( int bottom, int top )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source interval( javax.olap.sourcemodel.NumberSource bottom, javax.olap.sourcemodel.NumberSource top )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, boolean comparison )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, boolean[] comparison )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, java.util.Date comparison )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, java.util.Date[] comparison )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, double comparison )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, double[] comparison )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, float comparison )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, float[] comparison )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, int comparison )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, int[] comparison )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, short comparison )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, short[] comparison )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, javax.olap.sourcemodel.Source comparison )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, javax.olap.sourcemodel.Source comparison, boolean visible )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, javax.olap.sourcemodel.Source comparison, int comparisonRule )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, javax.olap.sourcemodel.Source comparison, int comparisonRule, boolean visible )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, java.lang.String comparison )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source join( javax.olap.sourcemodel.Source joined, java.lang.String[] comparison )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source joinHidden( javax.olap.sourcemodel.Source joined )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source last()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource le( javax.olap.sourcemodel.Source rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource lt( javax.olap.sourcemodel.Source rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source movingInterval( int bottom, int top )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source movingInterval( javax.olap.sourcemodel.NumberSource bottom, javax.olap.sourcemodel.NumberSource top )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.BooleanSource ne( javax.olap.sourcemodel.Source rhs )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source offset( int offset )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source offset( javax.olap.sourcemodel.NumberSource offset )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource position()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource positionOfValue( javax.olap.sourcemodel.Source value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource positionOfValues( javax.olap.sourcemodel.Source values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource positionOfValues( javax.olap.sourcemodel.Source[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source remove( javax.olap.sourcemodel.BooleanSource filter )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source removeValue( javax.olap.sourcemodel.Source value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source removeValues( javax.olap.sourcemodel.Source values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source removeValues( javax.olap.sourcemodel.Source[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source select( javax.olap.sourcemodel.BooleanSource filter )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source selectValue( javax.olap.sourcemodel.Source value )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source selectValues( javax.olap.sourcemodel.Source values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source selectValues( javax.olap.sourcemodel.Source[] values )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source sortAscending()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source sortAscending( javax.olap.sourcemodel.Source sortValue )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source sortDescending()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source sortDescending( javax.olap.sourcemodel.Source sortValue )
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource toDoubleSource()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource toFloatSource()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource toIntegerSource()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.NumberSource toShortSource()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.StringSource toStringSource()
    throws javax.olap.OLAPException;

  public javax.olap.sourcemodel.Source value()
    throws javax.olap.OLAPException;

}
