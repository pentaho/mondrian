package org. omg. cwm. foundation. softwaredeployment; 
import java. util. List; import java. util. Collection; 
import org. omg. cwm. foundation. typemapping.*; import org. omg. cwm. objectmodel. core.*; 
import org. omg. cwm. foundation. businessinformation.*; 
public interface Machine extends Namespace { 


// class scalar attributes 
public String getIpAddress(); 
public void setIpAddress( String input); 
public String getHostName(); 
public void setHostName( String input); 
public String getMachineID(); 
public void setMachineID( String input); 


// class references 
public void setSite( Site input); 
public Site getSite(); 
public void setDeployedComponent( Collection input); 
public Collection getDeployedComponent(); 
public void removeDeployedComponent( DeployedComponent input); 
// class operations 
} 


