/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.businessinformation;



public interface ContactTelephone
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.businessinformation.Telephone telephone, org.omg.java.cwm.foundation.businessinformation.Contact contact )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getContact( org.omg.java.cwm.foundation.businessinformation.Telephone telephone )
    throws javax.jmi.reflect.JmiException;

  public java.util.List getTelephone( org.omg.java.cwm.foundation.businessinformation.Contact contact )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.businessinformation.Telephone telephone, org.omg.java.cwm.foundation.businessinformation.Contact contact )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.businessinformation.Telephone telephone, org.omg.java.cwm.foundation.businessinformation.Contact contact )
    throws javax.jmi.reflect.JmiException;

}
