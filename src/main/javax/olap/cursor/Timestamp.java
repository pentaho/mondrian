package javax. olap. cursor;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import java. io.*;
import javax. olap. query. querycoremodel.*; import java. math.*;
import java. util.*; import javax. olap. query. querytransaction.*;


public interface Timestamp extends NamedObject {
// class scalar attributes
// class references // class operations
public void Timestamp( long time) throws OLAPException; public Timestamp valueOf( String s) throws OLAPException;
public String toString(); public int getNanos() throws OLAPException;
public void setNanos( int n) throws OLAPException; public boolean equals( Timestamp ts);
public boolean equals( Object ts); public boolean before( Timestamp ts) throws OLAPException;
public boolean after( Timestamp ts) throws OLAPException;
}


