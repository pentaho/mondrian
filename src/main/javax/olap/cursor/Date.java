package javax. olap. cursor;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import java. io.*;
import javax. olap. query. querycoremodel.*;
import java. math.*; import java. util.*;
import javax. olap. query. querytransaction.*;
public interface Date extends NamedObject {


// class scalar attributes
// class references // class operations
public void Date( long date) throws OLAPException;
public void setTime( long date) throws OLAPException;
public void valueOf( String s) throws OLAPException;
public String toString();
}


