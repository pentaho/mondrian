package org. omg. cwm. foundation. expressions; 
import java. util. List; import java. util. Collection; 
import org. omg. cwm. objectmodel. core.*; 
public interface FeatureNode extends ExpressionNode { 


// class scalar attributes 
// class references 
public void setArgument( Collection input); 


public List getArgument(); 
public void removeArgument( ExpressionNode input); 
public void moveArgumentBefore( ExpressionNode before, ExpressionNode input); 


public void moveArgumentAfter( ExpressionNode before, ExpressionNode input); 
public void setFeature( Feature input); 
public Feature getFeature(); 
// class operations 
} 

