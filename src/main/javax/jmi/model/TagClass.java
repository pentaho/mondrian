package javax.jmi.model;

import javax.jmi.reflect.*;

public interface TagClass extends RefClass {
    public Tag createTag();
    public Tag createTag(String name, String annotation, String tagId, java.util.List values);
}
