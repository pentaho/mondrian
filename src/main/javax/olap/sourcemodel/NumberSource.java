package javax. olap. sourcemodel;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import javax. jmi. reflect.*;


public interface NumberSource extends Source {
// class scalar attributes
// class references // class operations
public NumberSource abs() throws OLAPException; public NumberSource appendValue( double appendValue) throws
OLAPException; public NumberSource appendValue( float appendValue) throws
OLAPException; public NumberSource appendValue( int appendValue) throws
OLAPException; public NumberSource appendValue( short appendValue) throws
OLAPException; public NumberSource appendValues( double[] appendValues) throws
OLAPException; public NumberSource appendValues( float[] appendValues) throws
OLAPException; public NumberSource appendValues( int[] appendValues) throws
OLAPException; public NumberSource appendValues( short[] appendValues) throws
OLAPException; public NumberSource arccos() throws OLAPException;
public NumberSource arcsin() throws OLAPException; public NumberSource arctan() throws OLAPException;
public NumberSource average() throws OLAPException; public NumberSource average( boolean noValueAsZero) throws
OLAPException; public NumberSource cos() throws OLAPException;
public NumberSource cosh() throws OLAPException; public NumberSource div( double rhs) throws OLAPException;
public NumberSource div( float rhs) throws OLAPException; public NumberSource div( int rhs) throws OLAPException;
public NumberSource div( NumberSource rhs) throws OLAPException; public NumberSource div( short rhs) throws OLAPException;
public BooleanSource eq( double rhs) throws OLAPException; public BooleanSource eq( float rhs) throws OLAPException;
public BooleanSource eq( int rhs) throws OLAPException; public BooleanSource eq( short rhs) throws OLAPException;
public BooleanSource ge( double rhs) throws OLAPException; public BooleanSource ge( float rhs) throws OLAPException;
public BooleanSource ge( int rhs) throws OLAPException; public BooleanSource ge( short rhs) throws OLAPException;
public BooleanSource gt( double rhs) throws OLAPException; public BooleanSource gt( float rhs) throws OLAPException;
public BooleanSource gt( int rhs) throws OLAPException; public BooleanSource gt( short rhs) throws OLAPException;
public NumberSource intpart() throws OLAPException; public BooleanSource le( double rhs) throws OLAPException;
public BooleanSource le( float rhs) throws OLAPException; public BooleanSource le( int rhs) throws OLAPException;
public BooleanSource le( short rhs) throws OLAPException; public NumberSource log() throws OLAPException;
public BooleanSource lt( double rhs) throws OLAPException; public BooleanSource lt( float rhs) throws OLAPException;
public BooleanSource lt( int rhs) throws OLAPException; public BooleanSource lt( short rhs) throws OLAPException;
public NumberSource maximum() throws OLAPException; public NumberSource maximum( boolean noValueAsZero) throws
OLAPException; public NumberSource minimum() throws OLAPException;
public NumberSource minimum( boolean noValueAsZero) throws OLAPException;
public NumberSource minus( double rhs) throws OLAPException; public NumberSource minus( float rhs) throws OLAPException;
public NumberSource minus( int rhs) throws OLAPException; public NumberSource minus( short rhs) throws OLAPException;
public NumberSource minus( NumberSource rhs) throws OLAPException; public BooleanSource ne( double rhs) throws OLAPException;
public BooleanSource ne( float rhs) throws OLAPException; public BooleanSource ne( int rhs) throws OLAPException;
public BooleanSource ne( short rhs) throws OLAPException; public NumberSource negate() throws OLAPException;
public NumberSource plus( double rhs) throws OLAPException; public NumberSource plus( float rhs) throws OLAPException;
public NumberSource plus( int rhs) throws OLAPException; public NumberSource plus( NumberSource rhs) throws OLAPException;
public NumberSource plus( short rhs) throws OLAPException; public NumberSource positionOfValue( double value) throws
OLAPException; public NumberSource positionOfValue( float value) throws
OLAPException; public NumberSource positionOfValue( int value) throws OLAPException;
public NumberSource positionOfValue( short value) throws OLAPException;
public NumberSource positionOfValues( double[] values) throws OLAPException;
public NumberSource positionOfValues( float[] values) throws OLAPException;
public NumberSource positionOfValues( int[] values) throws OLAPException;
public NumberSource positionOfValues( short[] values) throws OLAPException;
public NumberSource pow( double rhs) throws OLAPException; public NumberSource pow( float rhs) throws OLAPException;
public NumberSource pow( int rhs) throws OLAPException; public NumberSource pow( NumberSource rhs) throws OLAPException;
public NumberSource pow( short rhs) throws OLAPException; public NumberSource rem( double rhs) throws OLAPException;
public NumberSource rem( float rhs) throws OLAPException; public NumberSource rem( int rhs) throws OLAPException;
public NumberSource rem( NumberSource rhs) throws OLAPException; public NumberSource rem( short rhs) throws OLAPException;
public NumberSource removeValue( double value) throws OLAPException;
public NumberSource removeValue( float value) throws OLAPException; public NumberSource removeValue( int value) throws OLAPException;
public NumberSource removeValue( short value) throws OLAPException; public NumberSource removeValues( double[] values) throws
OLAPException; public NumberSource removeValues( float[] values) throws
OLAPException; public NumberSource removeValues( int[] values) throws OLAPException;
public NumberSource removeValues( short[] values) throws OLAPException;
public NumberSource round( double multiple) throws OLAPException; public NumberSource round( float multiple) throws OLAPException;
public NumberSource round( int multiple) throws OLAPException; public NumberSource round( NumberSource multiple) throws
OLAPException; public NumberSource round( short multiple) throws OLAPException;
public NumberSource selectValue( double value) throws OLAPException; public NumberSource selectValue( float value) throws OLAPException;
public NumberSource selectValue( int value) throws OLAPException; public NumberSource selectValue( short value) throws OLAPException;
public NumberSource selectValues( double[] values) throws OLAPException;
public NumberSource selectValues( float[] values) throws OLAPException;
public NumberSource selectValues( int[] values) throws OLAPException; public NumberSource selectValues( short[] value) throws OLAPException;
public NumberSource sin() throws OLAPException; public NumberSource sinh() throws OLAPException;
public NumberSource sqrt() throws OLAPException; public NumberSource stdev() throws OLAPException;
public NumberSource stdev( boolean noValueAsZero) throws OLAPException;
public NumberSource tan() throws OLAPException; public NumberSource tanh() throws OLAPException;
public NumberSource times( double rhs) throws OLAPException; public NumberSource times( float rhs) throws OLAPException;
public NumberSource times( int rhs) throws OLAPException; public NumberSource times( NumberSource rhs) throws OLAPException;
public NumberSource times( short rhs) throws OLAPException; public NumberSource total() throws OLAPException;
public NumberSource total( boolean noValueAsZero) throws OLAPException;
}

