package org. omg. cwm. foundation. softwaredeployment; 
import java. util. List; import java. util. Collection; 
import org. omg. cwm. foundation. typemapping.*; import org. omg. cwm. objectmodel. core.*; 
import org. omg. cwm. foundation. businessinformation.*; 
public interface DataProvider extends DataManager { 


// class scalar attributes 
// class references 
public void setResourceConnection( Collection input); 
public Collection getResourceConnection(); 
public void removeResourceConnection( ProviderConnection input); 
// class operations 
} 

