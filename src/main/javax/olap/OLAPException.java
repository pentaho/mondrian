package javax. olap;
import java. util. List; import java. util. Collection;


public class OLAPException extends java. lang. Exception {
// class scalar attributes
// class references // class operations
public OLAPException() throws OLAPException {
super(); }


public OLAPException( String reason) throws OLAPException {
super( reason); }


public OLAPException( String reason, String OLAPState) throws OLAPException
{ super( reason);
}
public OLAPException( String reason, String OLAPState, int vendorCode) throws OLAPException
{ super( reason);
}
public String getOLAPState() throws OLAPException {
return( new String(" return implementation of error text")); }


public int getErrorCode() throws OLAPException {
int retval = 0; // retval should be populated by the implementation
// to return the integer errorcode
return( retval);
}
public OLAPException getNextException() throws OLAPException {
return( new OLAPException()); // this should return the next exception,
// this code will be replaced by the implementation
// to provide the next exception
}


public void setNextException( OLAPException exception) throws OLAPException
{
} }


