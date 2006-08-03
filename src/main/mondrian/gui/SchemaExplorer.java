/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1999-2002 Kana Software, Inc.
// Copyright (C) 2001-2005 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// Created on October 2, 2002, 5:42 PM
// Modified on 15-Jun-2003 by ebengtso
//
*/

package mondrian.gui;

import mondrian.olap.MondrianDef;
import mondrian.olap.MondrianDef.Schema;

import javax.swing.*;
import javax.swing.border.EtchedBorder;
import javax.swing.event.CellEditorListener;
import javax.swing.event.ChangeEvent;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellRenderer;
import javax.swing.tree.TreePath;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.util.ResourceBundle;

import org.eigenbase.xom.XOMUtil;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.NodeDef;

/**
 *
 * @author  sean
 * @version $Id$
 */
public class SchemaExplorer extends javax.swing.JPanel implements TreeSelectionListener, CellEditorListener
{
    private MondrianDef.Schema schema;
    private SchemaTreeModel model;
    private SchemaTreeCellRenderer renderer;
    private File schemaFile;
    private JTreeUpdater updater;
    private final ClassLoader myClassLoader;

    /** Creates new form SchemaExplorer */
    public SchemaExplorer()
    {
        myClassLoader = this.getClass().getClassLoader();
        initComponents();
    }

    public SchemaExplorer(File f)
    {
        this();
        try
        {
            Parser xmlParser = XOMUtil.createDefaultParser();
            this.schemaFile = f;

            schema = new MondrianDef.Schema(xmlParser.parse(schemaFile.toURL()));

            renderer = new SchemaTreeCellRenderer();
            model = new SchemaTreeModel(schema);
            tree.setModel(model);
            tree.setCellRenderer(renderer);
            tree.addTreeSelectionListener(this);
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor();
            spce.addCellEditorListener(this);
            propertyTable.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer();
            propertyTable.setDefaultRenderer(Object.class, spcr);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
    }

    /** This method is called from within the constructor to
     * initialize the form.
     */
    private void initComponents()
    {
        jSplitPane1 = new JSplitPane();
        jPanel1 = new JPanel();
        jScrollPane2 = new JScrollPane();
        propertyTable = new JTable();
        targetLabel = new javax.swing.JLabel();
        jPanel2 = new JPanel();
        jScrollPane1 = new JScrollPane();
        tree = new JTree();
        jToolBar1 = new JToolBar();
        addCubeButton = new JButton();
        addDimensionButton = new JButton();
        addMeasureButton = new JButton();
        addLevelButton = new JButton();
        addPropertyButton = new JButton();
        cutButton = new JButton();
        copyButton = new JButton();
        pasteButton = new JButton();

        setLayout(new BorderLayout());

        jSplitPane1.setDividerLocation(200);
        jPanel1.setLayout(new BorderLayout());

        propertyTable.setModel(new DefaultTableModel(new Object[][] {
        }, new String[] { "Property", "Value" })
        {
            Class[] types = new Class[] { java.lang.String.class, java.lang.Object.class };
            boolean[] canEdit = new boolean[] { false, true };

            public Class getColumnClass(int columnIndex)
            {
                return types[columnIndex];
            }

            public boolean isCellEditable(int rowIndex, int columnIndex)
            {
                return canEdit[columnIndex];
            }
        });
        jScrollPane2.setViewportView(propertyTable);

        jPanel1.add(jScrollPane2, java.awt.BorderLayout.CENTER);

        targetLabel.setFont(new Font("Dialog", 1, 14));
        targetLabel.setForeground((Color) UIManager.getDefaults().get("CheckBoxMenuItem.acceleratorForeground"));
        targetLabel.setHorizontalAlignment(SwingConstants.CENTER);
        targetLabel.setText("Schema");
        targetLabel.setBorder(new EtchedBorder());
        jPanel1.add(targetLabel, java.awt.BorderLayout.NORTH);

        jSplitPane1.setRightComponent(jPanel1);

        jPanel2.setLayout(new java.awt.BorderLayout());

        jScrollPane1.setViewportView(tree);

        jPanel2.add(jScrollPane1, java.awt.BorderLayout.CENTER);

        jSplitPane1.setLeftComponent(jPanel2);

        //========================================================
        // actions
        //========================================================
        addCube = new AbstractAction("Add cube")
        {
            public void actionPerformed(ActionEvent e)
            {
                addCube(e);
            }
        };
        addDimension = new AbstractAction("Add Dimension")
        {
            public void actionPerformed(ActionEvent e)
            {
                addDimension(e);
            }
        };
        addMeasure = new AbstractAction("Add Measure")
        {
            public void actionPerformed(ActionEvent e)
            {
                addMeasure(e);
            }
        };
        addLevel = new AbstractAction("Add Level")
        {
            public void actionPerformed(ActionEvent e)
            {
                addLevel(e);
            }
        };
        addProperty = new AbstractAction("Add Property")
        {
            public void actionPerformed(ActionEvent e)
            {
                addProperty(e);
            }
        };

        ResourceBundle resources = ResourceBundle.getBundle("mondrian.gui.resources.gui");

        //========================================================
        // toolbar buttons
        //========================================================
        addCubeButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addCube"))));
        addCubeButton.setToolTipText("Add Cube");
        addCubeButton.addActionListener(addCube);

        addDimensionButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addDimension"))));
        addDimensionButton.setToolTipText("Add Dimension");
        addDimensionButton.addActionListener(addDimension);

        addMeasureButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addMeasure"))));
        addMeasureButton.setToolTipText("Add Measure");
        addMeasureButton.addActionListener(addMeasure);

        addLevelButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addLevel"))));
        addLevelButton.setToolTipText("Add Level");
        addLevelButton.addActionListener(addLevel);

        addPropertyButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addProperty"))));
        addPropertyButton.setToolTipText("Add Property");
        addPropertyButton.addActionListener(addProperty);

        cutButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/Cut24.gif")));
        cutButton.setToolTipText("Cut");

        copyButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/Copy24.gif")));
        copyButton.setToolTipText("Copy");

        pasteButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/Paste24.gif")));
        pasteButton.setToolTipText("Paste");

        jToolBar1.add(addCubeButton);
        jToolBar1.add(addDimensionButton);
        jToolBar1.add(addMeasureButton);
        jToolBar1.add(addLevelButton);
        jToolBar1.add(addPropertyButton);
        jToolBar1.add(cutButton);
        jToolBar1.add(copyButton);
        jToolBar1.add(pasteButton);

        //========================================================
        // popup menu
        //========================================================
        jPopupMenu = new JPopupMenu();

        //========================================================
        // tree mouse listener
        //========================================================
        tree.addMouseListener(new PopupTrigger());

        //========================================================
        // jpanel
        //========================================================
        this.add(jSplitPane1, java.awt.BorderLayout.CENTER);
        this.add(jToolBar1, java.awt.BorderLayout.NORTH);

        updater = new JTreeUpdater(tree);

    }

    /**
     * @param evt
     */
    protected void addCube(ActionEvent evt)
    {
        MondrianDef.Schema schema = (Schema) tree.getModel().getRoot();
        MondrianDef.Cube cube = new MondrianDef.Cube();
        cube.name = "New Cube " + schema.cubes.length;

        cube.dimensions = new MondrianDef.Dimension[0];
        cube.measures = new MondrianDef.Measure[0];
        cube.fact = new MondrianDef.Table();

        //add cube to schema
        NodeDef[] temp = schema.cubes;
        schema.cubes = new MondrianDef.Cube[temp.length + 1];
        for (int _i = 0; _i < temp.length; _i++)
        {
            schema.cubes[_i] = (MondrianDef.Cube) temp[_i];
        }
        schema.cubes[schema.cubes.length - 1] = cube;
        refreshTree(tree.getSelectionPath());

    }

    /**
     * Updates the tree display after an Add / Delete operation.
     */
    private void refreshTree(TreePath path)
    {
        updater.update();
    }

    /**
     * @param evt
     */
    protected void addMeasure(ActionEvent evt)
    {
        Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianDef.Cube))
            return;
        MondrianDef.Cube cube = (MondrianDef.Cube) path;

        MondrianDef.Measure measure = new MondrianDef.Measure();
        measure.name = "New Measure " + cube.measures.length;
        //add cube to schema
        NodeDef[] temp = cube.measures;
        cube.measures = new MondrianDef.Measure[temp.length + 1];
        for (int i = 0; i < temp.length; i++)
            cube.measures[i] = (MondrianDef.Measure) temp[i];

        cube.measures[cube.measures.length - 1] = measure;

        refreshTree(tree.getSelectionPath());
    }

    /**
     * @param evt
     */
    protected void addDimension(ActionEvent evt)
    {
        Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianDef.Cube))
            return;

        MondrianDef.Cube cube = (MondrianDef.Cube) path;

        MondrianDef.Dimension dimension = new MondrianDef.Dimension();
        dimension.name = "New Dimension " + cube.dimensions.length;
        dimension.hierarchies = new MondrianDef.Hierarchy[1];
        dimension.hierarchies[0] = new MondrianDef.Hierarchy();
        dimension.hierarchies[0].hasAll = new Boolean(false);
        dimension.hierarchies[0].levels = new MondrianDef.Level[0];
        dimension.hierarchies[0].memberReaderParameters = new MondrianDef.MemberReaderParameter[0];
        dimension.hierarchies[0].relation = new MondrianDef.Join();

        //add cube to schema
        NodeDef[] temp = cube.dimensions;
        cube.dimensions = new MondrianDef.CubeDimension[temp.length + 1];
        for (int i = 0; i < temp.length; i++)
            cube.dimensions[i] = (MondrianDef.CubeDimension) temp[i];

        cube.dimensions[cube.dimensions.length - 1] = dimension;

        refreshTree(tree.getSelectionPath());
    }

    /**
     * @param evt
     */
    protected void addLevel(ActionEvent evt)
    {
        Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianDef.Hierarchy))
            return;

        MondrianDef.Hierarchy hierarchy = (MondrianDef.Hierarchy) path;

        MondrianDef.Level level = new MondrianDef.Level();
        level.uniqueMembers = new Boolean(false);
        level.name = "New Level " + hierarchy.levels.length;
        level.properties = new MondrianDef.Property[0];
        level.nameExp = new MondrianDef.NameExpression();
        level.nameExp.expressions = new MondrianDef.SQL[1];
        level.nameExp.expressions[0] = new MondrianDef.SQL();
        level.ordinalExp = new MondrianDef.OrdinalExpression();
        level.ordinalExp.expressions = new MondrianDef.SQL[1];
        level.ordinalExp.expressions[0] = new MondrianDef.SQL();
        //dimension.hierarchies[0].memberReaderParameters[0] = new MondrianDef.Parameter();

        //add cube to schema
        NodeDef[] temp = hierarchy.levels;
        hierarchy.levels = new MondrianDef.Level[temp.length + 1];
        for (int i = 0; i < temp.length; i++)
            hierarchy.levels[i] = (MondrianDef.Level) temp[i];

        hierarchy.levels[hierarchy.levels.length - 1] = level;

        refreshTree(tree.getSelectionPath());
    }

    /**
     * @param evt
     */
    protected void addProperty(ActionEvent evt)
    {
        Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianDef.Level))
            return;

        MondrianDef.Level level = (MondrianDef.Level) path;

        MondrianDef.Property property = new MondrianDef.Property();
        property.name = "New Property " + level.properties.length;

        //add cube to schema
        NodeDef[] temp = level.properties;
        level.properties = new MondrianDef.Property[temp.length + 1];
        for (int i = 0; i < temp.length; i++)
            level.properties[i] = (MondrianDef.Property) temp[i];

        level.properties[level.properties.length - 1] = property;

        refreshTree(tree.getSelectionPath());
    }
    public MondrianDef.Schema getSchema()
    {
        return this.schema;
    }

    /**
     * returns the schema file
     * @return File
     */
    public File getSchemaFile()
    {
        return this.schemaFile;
    }

    /**
     * sets the schema file
     * @param f
     */
    public void setSchemaFile(File f)
    {
        this.schemaFile = f;
    }

    /**
     * Called whenever the value of the selection changes.
     * @param e the event that characterizes the change.
     *
     */
    public void valueChanged(TreeSelectionEvent e)
    {
        Object o = e.getPath().getLastPathComponent();
        String[] pNames = DEF_DEFAULT;
        if (o instanceof MondrianDef.Column)
        {
            pNames = DEF_COLUMN;
            targetLabel.setText(LBL_COLUMN);
        }
        else if (o instanceof MondrianDef.Cube)
        {
            pNames = DEF_CUBE;
            targetLabel.setText(LBL_CUBE);
        }
        else if (o instanceof MondrianDef.Dimension)
        {
            pNames = DEF_DIMENSION;
            targetLabel.setText(LBL_DIMENSION);
        }
        else if (o instanceof MondrianDef.DimensionUsage)
        {
            pNames = DEF_DIMENSION_USAGE;
            targetLabel.setText(LBL_DIMENSION_USAGE);
        }
        else if (o instanceof MondrianDef.ExpressionView)
        {
            pNames = DEF_EXPRESSION_VIEW;
            targetLabel.setText(LBL_EXPRESSION_VIEW);
        }
        else if (o instanceof MondrianDef.Hierarchy)
        {
            pNames = DEF_HIERARCHY;
            targetLabel.setText(LBL_HIERARCHY);
        }
        else if (o instanceof MondrianDef.Join)
        {
            pNames = DEF_JOIN;
            targetLabel.setText(LBL_JOIN);
        }
        else if (o instanceof MondrianDef.Level)
        {
            pNames = DEF_LEVEL;
            targetLabel.setText(LBL_LEVEL);
        }
        else if (o instanceof MondrianDef.Measure)
        {
            pNames = DEF_MEASURE;
            targetLabel.setText(LBL_MEASURE);
        }
        else if (o instanceof MondrianDef.MemberReaderParameter)
        {
            pNames = DEF_PARAMETER;
            targetLabel.setText(LBL_PARAMETER);
        }
        else if (o instanceof MondrianDef.Property)
        {
            pNames = DEF_PROPERTY;
            targetLabel.setText(LBL_PROPERTY);
        }
        else if (o instanceof MondrianDef.Schema)
        {
            pNames = DEF_SCHEMA;
            targetLabel.setText(LBL_SCHEMA);
        }
        else if (o instanceof MondrianDef.SQL)
        {
            pNames = DEF_SQL;
            targetLabel.setText(LBL_SQL);
        }
        else if (o instanceof MondrianDef.Table)
        {
            pNames = DEF_TABLE;
            targetLabel.setText(LBL_TABLE);
        }
        else if (o instanceof MondrianDef.View)
        {
            pNames = DEF_VIEW;
            targetLabel.setText(LBL_VIEW);
        }
        else if (o instanceof MondrianDef.VirtualCube)
        {
            pNames = DEF_VIRTUAL_CUBE;
            targetLabel.setText(LBL_VIRTUAL_CUBE);
        }
        else if (o instanceof MondrianDef.VirtualCubeDimension)
        {
            pNames = DEF_VIRTUAL_CUBE_DIMENSION;
            targetLabel.setText(LBL_VIRTUAL_CUBE_DIMENSION);
        }
        else if (o instanceof MondrianDef.VirtualCubeMeasure)
        {
            pNames = DEF_VIRTUAL_CUBE_MEASURE;
            targetLabel.setText(LBL_VIRTUAL_CUBE_MEASURE);
        }
        else
        {
            targetLabel.setText(LBL_UNKNOWN_TYPE);
        }
        PropertyTableModel ptm = new PropertyTableModel(o, pNames);
        propertyTable.setModel(ptm);

        for (int i = 0; i < propertyTable.getRowCount(); i++)
        {
            TableCellRenderer renderer = propertyTable.getCellRenderer(i, 1);
            Component comp = renderer.getTableCellRendererComponent(propertyTable, propertyTable.getValueAt(i, 1), false, false, i, 1);
            try
            {
                int height = comp.getMaximumSize().height;
                propertyTable.setRowHeight(i, height);
            }
            catch (Exception ea)
            {
            }

        }

    }

    /**
     * @see javax.swing.event.CellEditorListener#editingCanceled(ChangeEvent)
     */
    public void editingCanceled(ChangeEvent e)
    {
        updater.update();
    }

    /**
     * @see javax.swing.event.CellEditorListener#editingStopped(ChangeEvent)
     */
    public void editingStopped(ChangeEvent e)
    {
        updater.update();
    }

    class PopupTrigger extends MouseAdapter
    {
        public void mouseReleased(MouseEvent e)
        {
            if (e.isPopupTrigger())
            {
                int x = e.getX();
                int y = e.getY();
                TreePath path = tree.getPathForLocation(x, y);
                if (path != null)
                {
                    jPopupMenu.removeAll();
                    Object pathSelected = path.getLastPathComponent();
                    if (pathSelected instanceof MondrianDef.Schema)
                    {
                        jPopupMenu.add(addCube);
                    }
                    else if (pathSelected instanceof MondrianDef.Cube)
                    {
                        jPopupMenu.add(addDimension);
                        jPopupMenu.add(addMeasure);
                    }
                    else if (pathSelected instanceof MondrianDef.Hierarchy)
                    {
                        jPopupMenu.add(addLevel);
                    }
                    else if (pathSelected instanceof MondrianDef.Level)
                    {
                        jPopupMenu.add(addProperty);
                    }
                    else
                    {
                        return;
                    }
                    jPopupMenu.show(tree, x, y);
                }
            }
        }
    }

    public static final String[] DEF_DEFAULT = {};
    public static final String[] DEF_VIRTUAL_CUBE = { "name" };
    public static final String[] DEF_VIRTUAL_CUBE_MEASURE = { "name", "cubeName" };
    public static final String[] DEF_VIRTUAL_CUBE_DIMENSION = { "cubeName" };
    public static final String[] DEF_VIEW = { "alias" };
    public static final String[] DEF_TABLE = { "name", "alias", "schema" };
    public static final String[] DEF_RELATION = { "name" };
    public static final String[] DEF_SQL = { "cdata", "dialect" };
    public static final String[] DEF_SCHEMA = {};
    public static final String[] DEF_PROPERTY = { "name", "column", "type" };
    public static final String[] DEF_PARAMETER = { "name", "value" };
    public static final String[] DEF_MEASURE = { "name", "aggregator", "column", "formatString" };
    public static final String[] DEF_LEVEL = { "name", "column", "nameExp", "ordinalColumn", "ordinalExp", "table", "type", "uniqueMembers" };
    public static final String[] DEF_JOIN = { "left", "leftAlias", "leftKey", "right", "rightAlias", "rightKey" };
    public static final String[] DEF_HIERARCHY = { "hasAll", "defaultMember", "memberReaderClass", "primaryKey", "primaryKeyTable", "relation" };
    public static final String[] DEF_EXPRESSION_VIEW = {};
    public static final String[] DEF_DIMENSION_USAGE = { "name", "foreignKey", "source" };
    public static final String[] DEF_DIMENSION = { "name", "foreignKey" };
    public static final String[] DEF_CUBE = { "name", "fact" };
    public static final String[] DEF_COLUMN = { "name", "table" };

    private static final String LBL_COLUMN = "Column";
    private static final String LBL_CUBE = "Cube";
    private static final String LBL_DIMENSION = "Dimension";
    private static final String LBL_DIMENSION_USAGE = "Dimension Usage";
    private static final String LBL_EXPRESSION_VIEW = "Expression View";
    private static final String LBL_HIERARCHY = "Hierarchy";
    private static final String LBL_JOIN = "Join";
    private static final String LBL_LEVEL = "Level";
    private static final String LBL_MEASURE = "Measure";
    private static final String LBL_PARAMETER = "Parameter";
    private static final String LBL_PROPERTY = "Property";
    private static final String LBL_SCHEMA = "Schema";
    private static final String LBL_SQL = "SQL";
    private static final String LBL_TABLE = "Table";
    private static final String LBL_VIEW = "View";
    private static final String LBL_VIRTUAL_CUBE = "Virtual Cube";
    private static final String LBL_VIRTUAL_CUBE_DIMENSION = "Virtual Cube Dimension";
    private static final String LBL_VIRTUAL_CUBE_MEASURE = "Virtual Cube Measure";
    private static final String LBL_UNKNOWN_TYPE = "Unknown Type";

    private AbstractAction addCube;
    private AbstractAction addDimension;
    private AbstractAction addMeasure;
    private AbstractAction addLevel;
    private AbstractAction addProperty;

    private JTable propertyTable;
    private JPanel jPanel2;
    private JPanel jPanel1;
    private JButton addLevelButton;
    private JScrollPane jScrollPane2;
    private JScrollPane jScrollPane1;
    private JButton addPropertyButton;
    private JButton pasteButton;
    private JLabel targetLabel;
    private JTree tree;
    private JSplitPane jSplitPane1;
    private JButton addDimensionButton;
    private JButton cutButton;
    private JButton addMeasureButton;
    private JButton addCubeButton;
    private JButton copyButton;
    private JToolBar jToolBar1;
    private JPopupMenu jPopupMenu;

}
