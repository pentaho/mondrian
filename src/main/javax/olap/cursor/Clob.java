package javax. olap. cursor;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import java. io.*;
import javax. olap. query. querycoremodel.*; import java. math.*;
import java. util.*;
public interface Clob extends NamedObject {


// class scalar attributes
// class references // class operations
public long length() throws OLAPException;
public String getSubString( long arg0, int arg1) throws OLAPException;
public Reader getCharacterStream() throws OLAPException;
public InputStream getAsciiStream() throws OLAPException;
public long position( String arg0, long arg1) throws OLAPException;
public long position( Clob arg0, long arg1) throws OLAPException;
}


