package org. omg. cwm. foundation. softwaredeployment; 
import java. util. List; import java. util. Collection; 
import org. omg. cwm. foundation. typemapping.*; import org. omg. cwm. objectmodel. core.*; 
import org. omg. cwm. foundation. businessinformation.*; 
public interface Site extends Location { 


// class scalar attributes 
// class references
public void setContainingSite( Collection input); 
public Collection getContainingSite(); 
public void addContainingSite( Site input); 
public void removeContainingSite( Site input); 
// class operations 
} 
