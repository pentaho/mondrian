/*
 * Java(TM) OLAP Interface
 */
package javax.olap;


public class OLAPWarning extends OLAPException
{
  public OLAPWarning()
  {
    super();
  }
  
  public OLAPWarning(String reason)
  {
    super(reason);
  }
  
  public OLAPWarning(String reason, String OLAPState)
  {
    super(reason, OLAPState);
  }
  
  public OLAPWarning(String reason, String OLAPState, int vendorCode)
  {
    super(reason, OLAPState, vendorCode);
  }
}