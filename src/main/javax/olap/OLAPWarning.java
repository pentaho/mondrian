package javax. olap;
import java. util. List; import java. util. Collection;


public class OLAPWarning extends OLAPException {
// class scalar attributes
// class references // class operations
public OLAPWarning() throws OLAPException {
super(); }


public OLAPWarning( String reason) throws OLAPException {
super( reason); }


public OLAPWarning( String reason, String OLAPState) throws OLAPException
{ super( reason, OLAPState);
}
public OLAPWarning( String reason, String OLAPState, int vendorCode) throws OLAPException
{ super( reason, OLAPState, vendorCode);
} }
