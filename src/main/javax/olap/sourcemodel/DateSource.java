package javax. olap. sourcemodel;
import java. util. List; import java. util. Collection; 
import javax. olap. OLAPException; 
public interface DateSource extends Source { 


// class scalar attributes 
// class references // class operations 
public DateSource appendValue( java. util. Date appendValue) throws OLAPException; 
public DateSource appendValues( java. util. Date[] appendValues) throws OLAPException; 
public DateSource eq( java. util. Date rhs) throws OLAPException; public DateSource ge( java. util. Date rhs) throws OLAPException; 
public DateSource gt( java. util. Date rhs) throws OLAPException; public DateSource le( java. util. Date rhs) throws OLAPException; 
public DateSource lt( java. util. Date rhs) throws OLAPException; public DateSource ne( java. util. Date rhs) throws OLAPException; 
public DateSource plusDays( int rhs) throws OLAPException; public DateSource plusDays( NumberSource rhs) throws OLAPException; 
public DateSource plusMonths( int rhs) throws OLAPException; public DateSource plusMonths( NumberSource rhs) throws OLAPException; 
public DateSource positionOfValue( java. util. Date value) throws OLAPException; 
public DateSource positionOfValues( java. util. Date[] values) throws OLAPException; 
public DateSource removeValue( java. util. Date value) throws OLAPException; 
public DateSource removeValues( java. util. Date[] values) throws OLAPException; 
public DateSource selectValue( java. util. Date value) throws OLAPException; 
public DateSource selectValues( java. util. Date[] values) throws OLAPException; 
} 


