package org. omg. cwm. objectmodel. core; 
import java. util. List; import java. util. Collection; 


public interface TaggedValue extends Element { 
// class scalar attributes 
public String getTag(); 
public void setTag( String input); 
public void setValue( String input); 


// class references 
public void setModelElement( ModelElement input); 
public ModelElement getModelElement(); 
public void setStereotype( Stereotype input); 
public Stereotype getStereotype(); 
// class operations 
} 


