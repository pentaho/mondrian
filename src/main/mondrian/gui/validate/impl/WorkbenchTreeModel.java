package mondrian.gui.validate.impl;

import mondrian.gui.SchemaTreeModel;
import mondrian.gui.validate.TreeModel;

/**
 * Implementation of <code>TreeModel</code> for Workbench.
 * 
 * @author mlowery
 */
public class WorkbenchTreeModel implements TreeModel {

    private SchemaTreeModel schemaTreeModel;

    public WorkbenchTreeModel(SchemaTreeModel schemaTreeModel) {
        super();
        this.schemaTreeModel = schemaTreeModel;
    }

    public Object getChild(Object parent, int index) {
        return schemaTreeModel.getChild(parent, index);
    }

    public int getChildCount(Object parent) {
        return schemaTreeModel.getChildCount(parent);
    }

    public Object getRoot() {
        return schemaTreeModel.getRoot();
    }

}
