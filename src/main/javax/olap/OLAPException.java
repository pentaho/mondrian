/*
 * Java(TM) OLAP Interface
 */
package javax.olap;

public class OLAPException extends java.lang.Exception
{
  public OLAPException()
  {
    super();
  }
  
  public OLAPException(String reason)
  {
    super(reason);
  }
  
  public OLAPException(String reason, String OLAPState)
  {
    super(reason);
  }

  public OLAPException(String reason, String OLAPState, int vendorCode)
  {
    super(reason);
  }
  
  public String getOLAPState()
  {
    return(new String("return implementation of error text"));
  }
  
  public int getErrorCode()
  {
    int retval = 0;   //retval should be populated by the implementation
                      //to return the integer errorcode
    return(retval);
  }

  public OLAPException getNextException()
  {
    return(new OLAPException()); //this method returns the next exception,
                                 //this code will be replaced by the implementation
                                 //to provide the next exception
  }
  
  public  void  setNextException(OLAPException exception)
  {
    //this method sets the next exception,
    //this code will be replaced by the implementation
    //to set the next exception
  }
}
