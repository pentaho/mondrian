/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.datatypes;



public interface UnionMemberClass
extends javax.jmi.reflect.RefClass {

  public org.omg.java.cwm.foundation.datatypes.UnionMember createUnionMember( java.lang.String _name, org.omg.java.cwm.objectmodel.core.VisibilityKind _visibility, org.omg.java.cwm.objectmodel.core.ScopeKind _ownerScope, org.omg.java.cwm.objectmodel.core.ChangeableKind _changeability, org.omg.java.cwm.objectmodel.core.Multiplicity _multiplicity, org.omg.java.cwm.objectmodel.core.OrderingKind _ordering, org.omg.java.cwm.objectmodel.core.ScopeKind _targetScope, org.omg.java.cwm.objectmodel.core.Expression _initialValue, org.omg.java.cwm.objectmodel.core.Expression _memberCase, boolean _isDefault )
    throws javax.jmi.reflect.JmiException;

  public org.omg.java.cwm.foundation.datatypes.UnionMember createUnionMember();

}
