package org. omg. cwm. foundation. expressions;
import java. util. List; import java. util. Collection;
import org. omg. cwm. objectmodel. core.*;
public interface ExpressionNode extends Element {


// class scalar attributes
public Expression getExpression();
public void setExpression( Expression input);
// class references
public void setFeatureNode( FeatureNode input);
public FeatureNode getFeatureNode();
public void setType( Classifier input);
public Classifier getType();
// class operations
}

