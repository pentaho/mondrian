package javax. olap. cursor;
import java. util. List; import java. util. Collection;
import javax. olap. OLAPException; import java. io.*;
import javax. olap. query. querycoremodel.*; import java. math.*;
import java. util.*;
public interface Blob extends NamedObject {


// class scalar attributes
// class references // class operations
public long length() throws OLAPException;
public byte[] getBytes( long arg0, int arg1) throws OLAPException;
public InputStream getBinaryStream() throws OLAPException;
public long position( byte[] arg0, long arg1) throws OLAPException;
public long position( Blob arg0, long arg1) throws OLAPException;
}

