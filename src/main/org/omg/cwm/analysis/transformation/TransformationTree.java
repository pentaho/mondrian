package org. omg. cwm. analysis. transformation;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*; import org. omg. cwm. foundation. expressions.*;
import org. omg. cwm. foundation. softwaredeployment.*;
public interface TransformationTree extends Transformation {
// class scalar attributes
public String getType();
public void setType( String input);
public ExpressionNode getBody();
public void setBody( ExpressionNode input);


// class references // class operations
}


