package javax.jmi.model;

import javax.jmi.reflect.*;

public interface Aliases extends RefAssociation {
    public boolean exists(Import importer, Namespace imported);
    public java.util.Collection getImporter(Namespace imported);
    public Namespace getImported(Import importer);
    public boolean add(Import importer, Namespace imported);
    public boolean remove(Import importer, Namespace imported);
}
