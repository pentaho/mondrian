package javax.jmi.model;

import javax.jmi.reflect.*;

public interface ConstantClass extends RefClass {
    public Constant createConstant();
    public Constant createConstant(String name, String annotation, String value);
}
