package mondrian.gui;

import javax.swing.JMenuBar;

public interface WorkbenchMenubarPlugin {
    public void addItemsToMenubar(JMenuBar menubar);
    public void setWorkbench(Workbench workbench);
}
