package javax. olap. sourcemodel;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import javax. jmi. reflect.*;


public interface Source extends RefObject {
// class scalar attributes
// class references public void setSourceOutput( Collection input) throws OLAPException;
public Collection getSourceOutput() throws OLAPException;
public void addSourceOutput( Source input) throws OLAPException;
public void removeSourceOutput( Source input) throws OLAPException;
public void setSourceInput( Collection input) throws OLAPException;
public Collection getSourceInput() throws OLAPException;
public void addSourceInput( Source input) throws OLAPException;
public void removeSourceInput( Source input) throws OLAPException;
public void setType( Source input) throws OLAPException;
public Source getType() throws OLAPException;
// class operations public Source alias() throws OLAPException;
public Source appendValue( Source appendValue) throws OLAPException; public Source appendValues( Source appendValues) throws OLAPException;
public Source appendValues( Source[] appendValues) throws OLAPException;
public Source at( int position) throws OLAPException; public Source at( NumberSource position) throws OLAPException;
public NumberSource count() throws OLAPException; public NumberSource count( boolean includeNoValue) throws
OLAPException; public Source cumulativeInterval() throws OLAPException;
public Source cumulativeInterval( int offset) throws OLAPException; public Source cumulativeInterval( Source offset) throws OLAPException;
public Source distinct() throws OLAPException; public Source eq( Source rhs) throws OLAPException;
public Source extract() throws OLAPException; public Source findMatchFor( Source input) throws OLAPException;
public Source first() throws OLAPException; public BooleanSource ge( Source rhs) throws OLAPException;
public Source getDataType() throws OLAPException; public java. util. Set getInputs() throws OLAPException;
public java. util. List getOutputs() throws OLAPException; public void gt( Source rhs) throws OLAPException;
public BooleanSource hasValue() throws OLAPException; public BooleanSource in( Source list) throws OLAPException;
public Source interval( int bottom, int top) throws OLAPException; public Source interval( NumberSource bottom, NumberSource top) throws
OLAPException; public Source join( Source joined) throws OLAPException;
public Source join( Source joined, boolean comparison) throws OLAPException;
public Source join( Source joined, boolean[] comparison) throws OLAPException;
public Source join( Source joined, java. util. Date comparison) throws OLAPException;
public Source join( Source joined, java. util. Date[] comparison) throws OLAPException;
public Source join( Source joined, double comparison) throws OLAPException;
public Source join( Source joined, double[] comparison) throws OLAPException;
public Source join( Source joined, float comparison) throws OLAPException;
public Source join( Source joined, float[] comparison) throws OLAPException;
public Source join( Source joined, int comparison) throws OLAPException;
public Source join( Source joined, int[] comparison) throws OLAPException;
public Source join( Source joined, short comparison) throws OLAPException;
public Source join( Source joined, short[] comparison) throws OLAPException;
public Source join( Source joined, Source comparison) throws OLAPException;
public Source join( Source joined, Source comparison, boolean visible) throws OLAPException;
public Source join( Source joined, Source comparison, int comparisonRule) throws OLAPException;
public Source join( Source joined, Source comparison, int comparisonRule, boolean visible) throws OLAPException;
public Source join( Source joined, java. lang. String comparison) throws OLAPException;
public Source join( Source joined, java. lang. String[] comparison) throws OLAPException;
public Source joinHidden( Source joined) throws OLAPException; public Source last() throws OLAPException;
public BooleanSource le( Source rhs) throws OLAPException; public BooleanSource lt( Source rhs) throws OLAPException;
public Source movingInterval( int bottom, int top) throws OLAPException;
public Source movingInterval( NumberSource bottom, NumberSource top) throws OLAPException;
public BooleanSource ne( Source rhs) throws OLAPException; public Source offset( int offset) throws OLAPException;
public Source offset( NumberSource offset) throws OLAPException; public NumberSource position() throws OLAPException;
public NumberSource positionOfValue( Source value) throws OLAPException;
public NumberSource positionOfValues( Source values) throws OLAPException;
public NumberSource positionOfValues( Source[] values) throws OLAPException;
public Source remove( BooleanSource filter) throws OLAPException; public Source removeValue( Source value) throws OLAPException;
public Source removeValues( Source values) throws OLAPException; public Source removeValues( Source[] values) throws OLAPException;
public Source select( BooleanSource filter) throws OLAPException; public Source selectValue( Source value) throws OLAPException;
public Source selectValues( Source values) throws OLAPException; public Source selectValues( Source[] values) throws OLAPException;
public Source sortAscending() throws OLAPException;
public Source sortAscending( Source sortValue) throws OLAPException; public Source sortDescending() throws OLAPException;
public Source sortDescending( Source sortValue) throws OLAPException; public NumberSource toDoubleSource() throws OLAPException;
public NumberSource toFloatSource() throws OLAPException; public NumberSource toIntegerSource() throws OLAPException;
public NumberSource toShortSource() throws OLAPException; public StringSource toStringSource() throws OLAPException;
public Source value() throws OLAPException;
}

