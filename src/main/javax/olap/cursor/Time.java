package javax. olap. cursor; 
import java. util. List; import java. util. Collection; 
import javax. olap. OLAPException; import java. io.*; 
import javax. olap. query. querycoremodel.*; import java. math.*; 
import java. util.*; import javax. olap. query. querytransaction.*; 


public interface Time extends NamedObject { 
// class scalar attributes 
// class references // class operations 
public void Time( long time) throws OLAPException; public Time valueOf( String s) throws OLAPException; 
public String toString(); public void setTime( long time) throws OLAPException;
}