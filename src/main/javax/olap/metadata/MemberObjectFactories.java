/*
 * Java(TM) OLAP Interface
 */
package javax.olap.metadata;



public interface MemberObjectFactories {

	// ------------------------------------------------
	// -----   Interface Operations               -----
	// ------------------------------------------------

  public javax.olap.metadata.Member createMember( javax.olap.metadata.Dimension owner )
    throws javax.olap.OLAPException;

  public javax.olap.metadata.CurrentMember createCurrentMember( javax.olap.metadata.Dimension owner )
    throws javax.olap.OLAPException;

  public javax.olap.metadata.MemberList createMemberList( javax.olap.metadata.Dimension owner )
    throws javax.olap.OLAPException;

  public javax.olap.metadata.MemberValue createMemberValue( javax.olap.metadata.Member owner, org.omg.java.cwm.objectmodel.core.Attribute attribute )
    throws javax.olap.OLAPException;

}
