package javax. olap. sourcemodel;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException;
public interface BooleanSource extends Source {
// class scalar attributes
// class references // class operations
public BooleanSource and( boolean rhs) throws OLAPException; public BooleanSource and( BooleanSource rhs) throws OLAPException;
public BooleanSource appendValue( boolean appendValue) throws OLAPException;
public BooleanSource appendValues( boolean[] appendValues) throws OLAPException;
public BooleanSource eq( boolean rhs) throws OLAPException; public BooleanSource forAll() throws OLAPException;
public BooleanSource forAll( boolean noValueAsFalse) throws OLAPException;
public BooleanSource forAny() throws OLAPException; public BooleanSource forAny( boolean noValueAsFalse) throws
OLAPException; public BooleanSource forNone() throws OLAPException;
public BooleanSource forNone( boolean noValueAsFalse) throws OLAPException;
public BooleanSource ge( boolean rhs) throws OLAPException; public BooleanSource gt( boolean rhs) throws OLAPException;
public DateSource implies( java. util. Date ifTrue, java. util. Date ifFalse) throws OLAPException;
public DateSource implies( DateSource ifTrue, DateSource ifFalse) throws OLAPException;
public NumberSource implies( double ifTrue, double ifFalse) throws OLAPException;
public NumberSource implies( int ifTrue, int ifFalse) throws OLAPException;
public NumberSource implies( NumberSource ifTrue, NumberSource ifFalse) throws OLAPException;
public StringSource implies( StringSource ifTrue, StringSource ifFalse) throws OLAPException;
public StringSource implies( java. lang. String ifTrue, java. lang. String ifFalse) throws OLAPException;
public BooleanSource le( boolean rhs) throws OLAPException; public BooleanSource lt( boolean rhs) throws OLAPException;
public BooleanSource ne( boolean rhs) throws OLAPException; public BooleanSource not() throws OLAPException;
public BooleanSource or( boolean rhs) throws OLAPException; public BooleanSource positionOfValue( boolean value) throws
OLAPException; public BooleanSource positionOfValues( boolean[] values) throws
OLAPException; public BooleanSource removeValue( boolean value) throws OLAPException;
public BooleanSource removeValues( boolean[] values) throws OLAPException;
public BooleanSource selectValue( boolean value) throws OLAPException; public BooleanSource selectValues( boolean[] values) throws
OLAPException;
}

