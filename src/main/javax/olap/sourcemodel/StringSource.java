package javax. olap. sourcemodel;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import javax. jmi. reflect.*;


public interface StringSource extends Source {
// class scalar attributes
// class references // class operations
public StringSource appendValue( java. lang. String appendValue) throws OLAPException;
public StringSource appendValues( java. lang. String[] appendValues) throws OLAPException;
public BooleanSource eq( java. lang. String rhs) throws OLAPException; public BooleanSource ge( java. lang. String rhs) throws OLAPException;
public BooleanSource gt( java. lang. String rhs) throws OLAPException; public NumberSource indexOf( java. lang. String substring, int
fromIndex) throws OLAPException; public NumberSource indexOf( StringSource substring, NumberSource
fromIndex) throws OLAPException; public BooleanSource le( java. lang. String rhs) throws OLAPException;
public NumberSource length() throws OLAPException; public BooleanSource like( java. lang. String rhs) throws OLAPException;
public BooleanSource like( StringSource rhs) throws OLAPException; public BooleanSource lt( java. lang. String rhs) throws OLAPException;
public BooleanSource ne( java. lang. String rhs) throws OLAPException; public NumberSource positionOfValue( java. lang. String value) throws
OLAPException; public NumberSource positionOfValues( java. lang. String[] values)
throws OLAPException; public StringSource remove( int index, int length) throws
OLAPException; public StringSource remove( NumberSource index, NumberSource length)
throws OLAPException; public StringSource removeValue( java. lang. String value) throws
OLAPException; public StringSource removeValues( java. lang. String[] values) throws
OLAPException; public StringSource replace( StringSource oldString, StringSource
newString) throws OLAPException; public StringSource replace( java. lang. String oldString,
java. lang. String newString) throws OLAPException; public StringSource selectValue( java. lang. String value) throws
OLAPException; public StringSource selectValues( java. lang. String[] values) throws
OLAPException; public StringSource substring( int index, int length) throws
OLAPException; public StringSource substring( NumberSource index, NumberSource
length) throws OLAPException; public StringSource textFill( int width) throws OLAPException;
public StringSource textFill( NumberSource width) throws OLAPException;
public StringSource toLowercase() throws OLAPException; public StringSource toUppercase() throws OLAPException;
public StringSource trim() throws OLAPException; public StringSource trimLeading() throws OLAPException;
public StringSource trimTrailing() throws OLAPException;
}
