package javax. olap. cursor;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import java. io.*;
import javax. olap. query. querycoremodel.*; import java. math.*;
import java. util.*; import javax. olap. query. querytransaction.*;


public interface Cursor extends NamedObject {
// class scalar attributes
// class references // class operations
public Object clone();
public boolean equals( Object arg0);
public int hashCode();
}


