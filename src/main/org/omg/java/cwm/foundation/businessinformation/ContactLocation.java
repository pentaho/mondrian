/*
 * Java(TM) OLAP Interface
 */
package org.omg.java.cwm.foundation.businessinformation;



public interface ContactLocation
extends javax.jmi.reflect.RefAssociation {

  public boolean exists( org.omg.java.cwm.foundation.businessinformation.Location location, org.omg.java.cwm.foundation.businessinformation.Contact contact )
    throws javax.jmi.reflect.JmiException;

  public java.util.Collection getContact( org.omg.java.cwm.foundation.businessinformation.Location location )
    throws javax.jmi.reflect.JmiException;

  public java.util.List getLocation( org.omg.java.cwm.foundation.businessinformation.Contact contact )
    throws javax.jmi.reflect.JmiException;

  public boolean add( org.omg.java.cwm.foundation.businessinformation.Location location, org.omg.java.cwm.foundation.businessinformation.Contact contact )
    throws javax.jmi.reflect.JmiException;

  public boolean remove( org.omg.java.cwm.foundation.businessinformation.Location location, org.omg.java.cwm.foundation.businessinformation.Contact contact )
    throws javax.jmi.reflect.JmiException;

}
