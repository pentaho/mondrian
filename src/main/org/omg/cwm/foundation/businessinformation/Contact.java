package org. omg. cwm. foundation. businessinformation; 
import java. util. List; import java. util. Collection; 
import org. omg. cwm. objectmodel. core.*; 
public interface Contact extends ModelElement { 


// class scalar attributes 
// class references 
public void setEmail( Collection input); 


public List getEmail(); 
public void addEmail( Email input); 
public void removeEmail( Email input); 
public void addEmailBefore( Email before, Email input); 
public void addEmailAfter( Email before, Email input); 
public void moveEmailBefore( Email before, Email input); 
public void moveEmailAfter( Email before, Email input); 
public void setLocation( Collection input); 
public List getLocation(); 
public void addLocation( Location input); 
public void removeLocation( Location input); 
public void addLocationBefore( Location before, Location input); 
public void addLocationAfter( Location before, Location input); 
public void moveLocationBefore( Location before, Location input); 
public void moveLocationAfter( Location before, Location input); 
public void setResponsibleParty( Collection input); 
public Collection getResponsibleParty(); 
public void addResponsibleParty( ResponsibleParty input); 
public void removeResponsibleParty( ResponsibleParty input); 
public void setTelephone( Collection input);
public List getTelephone(); 
public void addTelephone( Telephone input); 
public void removeTelephone( Telephone input); 
public void addTelephoneBefore( Telephone before, Telephone input); 
public void addTelephoneAfter( Telephone before, Telephone input); 
public void moveTelephoneBefore( Telephone before, Telephone input); 
public void moveTelephoneAfter( Telephone before, Telephone input); 
public void setUrl( Collection input); 
public List getUrl(); 
public void addUrl( ResourceLocator input); 
public void removeUrl( ResourceLocator input); 
public void addUrlBefore( ResourceLocator before, ResourceLocator input); 


public void addUrlAfter( ResourceLocator before, ResourceLocator input); 
public void moveUrlBefore( ResourceLocator before, ResourceLocator input); 
public void moveUrlAfter( ResourceLocator before, ResourceLocator input); 
// class operations 
} 


