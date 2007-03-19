/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2006 Julian Hyde and others
// Copyright (C) 2006-2007 CINCOM SYSTEMS, INC.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// Created on October 2, 2002, 5:42 PM
// Modified on 15-Jun-2003 by ebengtso
*/
package mondrian.gui;

import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.InputEvent;
import java.awt.event.KeyAdapter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.EventObject;

import javax.swing.*;
import javax.swing.border.EtchedBorder;
import javax.swing.event.CellEditorListener;
import javax.swing.event.ChangeEvent;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableCellEditor;
import javax.swing.tree.TreePath;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ResourceBundle;
import javax.swing.event.TreeExpansionEvent;
import javax.swing.tree.TreeCellEditor;
import javax.swing.plaf.basic.BasicArrowButton;
import javax.swing.tree.DefaultTreeSelectionModel;

import org.eigenbase.xom.XOMUtil;
import org.eigenbase.xom.Parser;
import org.eigenbase.xom.NodeDef;
import org.eigenbase.xom.XOMException;

/**
 *
 * @author  sean
 * @version $Id$
 */
public class SchemaExplorer extends javax.swing.JPanel implements TreeSelectionListener, CellEditorListener {
    private MondrianGuiDef.Schema schema;
    private SchemaTreeModel model;
    private SchemaTreeCellRenderer renderer;
    private SchemaTreeCellEditor editor;
    private File schemaFile;
    private JTreeUpdater updater;
    private final ClassLoader myClassLoader;
    private boolean newFile;
    private boolean dirty = false;  //indicates file is without changes, dirty=true when some changes are made to the file
    private boolean dirtyFlag = false;  // indicates dirty status shown on title
    private JInternalFrame parentIFrame;
    private JDBCMetaData jdbcMetaData;
    private boolean editModeXML = false;
    private String errMsg = null;

    /** Creates new form SchemaExplorer */
    public SchemaExplorer() {
        myClassLoader = this.getClass().getClassLoader();
        initComponents();
    }

    public SchemaExplorer(File f, JDBCMetaData jdbcMetaData, boolean newFile, JInternalFrame parentIFrame) {
        this();

        //====XML editor
        try {
            jEditorPaneXML = new JEditorPane();//===new JEditorPane(f.toURL().toString());
            //jEditorPaneXML.setContentType("text/xml");
            //jEditorPaneXML = new JEditorPane("text/xml", "<h>jjo <nkl>sf</nkl>sdf </h>");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        jEditorPaneXML.setLayout(new java.awt.BorderLayout());
        jEditorPaneXML.setEditable(false);
        jScrollPaneXML = new JScrollPane(jEditorPaneXML);
        //===jScrollPaneXML.setViewportView(jEditorPaneXML);
        jPanelXML.setLayout(new java.awt.BorderLayout());
        jPanelXML.add(jScrollPaneXML, java.awt.BorderLayout.CENTER);
        jPanelXML.add(targetLabel2, java.awt.BorderLayout.NORTH);
        jPanelXML.add(validStatusLabel2, java.awt.BorderLayout.SOUTH);
        jPanelXML.setMaximumSize(jPanel1.getMaximumSize());
        jPanelXML.setPreferredSize(jPanel1.getPreferredSize());

        //jSplitPane1.setRightComponent(jPanelXML);

        databaseLabel.setText("Database -  " +jdbcMetaData.getDbCatalogName() + "  ("+ jdbcMetaData.getDatabaseProductName()+")");

        try {
            Parser xmlParser = XOMUtil.createDefaultParser();
            this.schemaFile = f;
            this.setNewFile(newFile);
            this.parentIFrame = parentIFrame;

            this.jdbcMetaData = jdbcMetaData;

            if (newFile) {
                schema = new MondrianGuiDef.Schema();
                schema.cubes = new MondrianGuiDef.Cube[0];
                schema.dimensions = new MondrianGuiDef.Dimension[0];
                schema.namedSets = new MondrianGuiDef.NamedSet[0];
                schema.roles = new MondrianGuiDef.Role[0];
                schema.userDefinedFunctions = new MondrianGuiDef.UserDefinedFunction[0];
                schema.virtualCubes = new MondrianGuiDef.VirtualCube[0];

                String sname = schemaFile.getName();
                int ext = sname.indexOf(".");
                if (ext != -1) {
                    schema.name = "New "+sname.substring(0,ext);
                }
            } else {
                try {
                    schema = new MondrianGuiDef.Schema(xmlParser.parse(schemaFile.toURL()));
                } catch(XOMException ex) {
                    // parsing error of the schema file causes default tree of colors etc. to be displayed in schema explorer
                    // initialize the schema to display an empty schema if you want to show schema explorer for file
                    // where parsing failed.
                    schema = new MondrianGuiDef.Schema();
                    schema.cubes = new MondrianGuiDef.Cube[0];
                    schema.dimensions = new MondrianGuiDef.Dimension[0];
                    schema.namedSets = new MondrianGuiDef.NamedSet[0];
                    schema.roles = new MondrianGuiDef.Role[0];
                    schema.userDefinedFunctions = new MondrianGuiDef.UserDefinedFunction[0];
                    schema.virtualCubes = new MondrianGuiDef.VirtualCube[0];

                    System.out.println("Exception  : Schema file parsing failed."+ex.getMessage());
                    errMsg = "Parsing Error: Could not open file\n"+schemaFile+".\n"+ex.getMessage();
                }
            }
            setTitle(); // sets title of i frame with schema name and file name

            //renderer = new SchemaTreeCellRenderer();
            renderer = new SchemaTreeCellRenderer(jdbcMetaData);
            model = new SchemaTreeModel(schema);
            tree.setModel(model);
            tree.setCellRenderer(renderer);
            tree.addTreeSelectionListener(this);

            JComboBox listEditor = new JComboBox( new String[] {"Join", "Table"} );
            listEditor.setToolTipText("select Join or Table Hierarchy");
            listEditor.setPreferredSize(new java.awt.Dimension(listEditor.getPreferredSize().width, 24)); //Do not remove this

            listEditor.addItemListener(new ItemListener() {
                public void itemStateChanged(ItemEvent e) {
                    tree.stopEditing();
                    TreePath tpath = tree.getSelectionPath(); //tree.getEditingPath();
                    if (tpath != null) {
                        TreePath parentpath = tpath.getParentPath();
                        if (parentpath != null) {
                            refreshTree(parentpath);
                        }
                    }
                }
            });

            TreeCellEditor comboEditor = new DefaultCellEditor(listEditor);

            editor = new SchemaTreeCellEditor(tree, renderer, comboEditor);
            tree.setCellEditor(editor);
            tree.setEditable(true);


            //SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor();
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor(jdbcMetaData);
            spce.addCellEditorListener(this);
            propertyTable.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer.attributeBackground = jScrollPane2.getBackground();    // to set background color of attribute columns
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer();
            propertyTable.setDefaultRenderer(Object.class, spcr);


             /*  Focus lost on table was supposed to save the last edited value in table model object
                but tabel focus lost is not called when tree selection is changed
            //propertyTable.addPropertyChangeListener("model", this);
            propertyTable.addFocusListener(new FocusAdapter() {
                public void focusGained(FocusEvent e) {System.out.println("====Table GAINED focus");}

                public void focusLost(FocusEvent e) {System.out.println("====Table LOST focus");
                        Object value = propertyTable.getCellEditor().getCellEditorValue();
                        propertyTable.setValueAt(value, propertyTable.getEditingRow(), propertyTable.getEditingColumn());
                }
            });
              */

            /* This line is supposed to save cell edited values when a user clicks anywhere outside the table
                // propertyTable.putClientProperty("terminateEditOnFocusLost", Boolean.TRUE);
             * But this does not save if a tree cell is selected , whereas it only saves if a click is done anywhere on let panel.
             */

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /** This method is called from within the constructor to
     * initialize the form.
     */
    private void initComponents() {

        ResourceBundle resources = ResourceBundle.getBundle("mondrian.gui.resources.gui");
        jPanelXML = new JPanel();
        jScrollPaneXML = new JScrollPane();

        footer = new JPanel();
        databaseLabel = new javax.swing.JLabel();

        jSeparator1 = new JSeparator();
        jSplitPane1 = new JSplitPane();
        jPanel1 = new JPanel();
        jScrollPane2 = new JScrollPane();
        //=============================================================
        // propertyTable includes changeSelection and processKeyEvent
        // processing for keyboard navigation
        //=============================================================
        propertyTable = new JTable() {
            public void changeSelection(int rowIndex, int columnIndex, boolean toggle, boolean extend) {
                if (columnIndex == 0) {
                    AWTEvent currentEvent = EventQueue.getCurrentEvent();
                    if(currentEvent instanceof KeyEvent) {
                        KeyEvent ke = (KeyEvent)currentEvent;
                        int kcode = ke.getKeyCode();
                        if (kcode == KeyEvent.VK_TAB) {
                            if ((ke.getModifiersEx() & InputEvent.SHIFT_DOWN_MASK) ==
                                    InputEvent.SHIFT_DOWN_MASK) {
                                rowIndex -= 1;
                                if (rowIndex < 0) {
                                    rowIndex = (propertyTable.getRowCount()) - 1;
                                }
                            }
                          setTableCellFocus(rowIndex);
                          return;
                        }
                    }
                }
                super.changeSelection(rowIndex, columnIndex, toggle, extend);
            }
            public void processKeyEvent(KeyEvent e) {
                int kcode = e.getKeyCode();
                if (kcode == KeyEvent.VK_UP || kcode == KeyEvent.VK_DOWN) {
                    int row = propertyTable.getSelectedRow();
                    setTableCellFocus(row);
                    return;
                }
                super.processKeyEvent(e);
            }
        };

        targetLabel = new javax.swing.JLabel();
        validStatusLabel = new javax.swing.JLabel();
        targetLabel2 = new javax.swing.JLabel();
        validStatusLabel2 = new javax.swing.JLabel();
        jPanel2 = new JPanel();
        jScrollPane1 = new JScrollPane();

    	tree = new JTree() {
    	    public String getToolTipText(MouseEvent evt) {
                String toggleMsg = "Double click to display Join/Table selection";
    	    	if (getRowForLocation(evt.getX(), evt.getY()) == -1) return null;
    	    	TreePath curPath = getPathForLocation(evt.getX(), evt.getY());
                Object o = curPath.getLastPathComponent();
                if (o instanceof MondrianGuiDef.Join) {
    	    	    return toggleMsg;
                }
                TreePath parentPath = curPath.getParentPath();
                if (parentPath != null) {
                    Object po = parentPath.getLastPathComponent();
                    if (o instanceof MondrianGuiDef.Table &&
                        (po instanceof MondrianGuiDef.Hierarchy ||
                            po instanceof MondrianGuiDef.Join)) {
                            return toggleMsg;
                    }
                }
                return null;
    	    }
    	};
        tree.getSelectionModel().setSelectionMode(DefaultTreeSelectionModel.SINGLE_TREE_SELECTION);

        ToolTipManager.sharedInstance().registerComponent(tree);

        jToolBar1 = new JToolBar();
        addCubeButton = new JButton();
        addDimensionButton = new JButton();
        addDimensionUsageButton = new JButton();
        addHierarchyButton = new JButton();
        addNamedSetButton = new JButton();
        addUserDefinedFunctionButton = new JButton();
        addRoleButton = new JButton();

        addMeasureButton = new JButton();
        addCalculatedMemberButton = new JButton();
        addLevelButton = new JButton();
        addPropertyButton = new JButton();

        addVirtualCubeButton = new JButton();
        addVirtualCubeDimensionButton = new JButton();
        addVirtualCubeMeasureButton = new JButton();

        cutButton = new JButton();
        copyButton = new JButton();
        pasteButton = new JButton();
        deleteButton = new JButton();
        editModeButton = new JToggleButton();

        setLayout(new BorderLayout());

        jSplitPane1.setDividerLocation(200);
        jPanel1.setLayout(new BorderLayout());

        propertyTable.setModel(new DefaultTableModel(new Object[][] {
        }, new String[] { "Attribute", "Value" }) {
            Class[] types = new Class[] { java.lang.String.class, java.lang.Object.class };
            boolean[] canEdit = new boolean[] { false, true };

            public Class getColumnClass(int columnIndex) {
                return types[columnIndex];
            }

            public boolean isCellEditable(int rowIndex, int columnIndex) {
                return canEdit[columnIndex];
            }
        });

        propertyTable.getTableHeader().setFont(new Font("Dialog", Font.BOLD, 12));  // setting property table column headers to bold

        jScrollPane2.setViewportView(propertyTable);

        jPanel1.add(jScrollPane2, java.awt.BorderLayout.CENTER);

        targetLabel.setFont(new Font("Dialog", 1, 14));
        targetLabel.setForeground((Color) UIManager.getDefaults().get("CheckBoxMenuItem.acceleratorForeground"));
        targetLabel.setHorizontalAlignment(SwingConstants.CENTER);
        targetLabel.setText("Schema");
        targetLabel.setBorder(new EtchedBorder());
        // up arrow button for property table heading
        jPanel3 = new JPanel();
        jPanel3.setLayout(new BorderLayout());
        BasicArrowButton arrowButtonUp = new BasicArrowButton(SwingConstants.NORTH);
        BasicArrowButton arrowButtonDown = new BasicArrowButton(SwingConstants.SOUTH);
        arrowButtonUp.setToolTipText("move to parent element");
        arrowButtonDown.setToolTipText("move to child element");
        arrowButtonUpAction = new AbstractAction("Arrow button up") {
            public void actionPerformed(ActionEvent e) {
                arrowButtonUpAction(e);
            }
        };
        arrowButtonDownAction = new AbstractAction("Arrow button down") {
            public void actionPerformed(ActionEvent e) {
                arrowButtonDownAction(e);
            }
        };
        arrowButtonUp.addActionListener(arrowButtonUpAction);
        arrowButtonDown.addActionListener(arrowButtonDownAction);
        jPanel3.add(arrowButtonDown, java.awt.BorderLayout.EAST);
        jPanel3.add(arrowButtonUp, java.awt.BorderLayout.WEST);
        jPanel3.add(targetLabel, java.awt.BorderLayout.CENTER);

        jPanel1.add(jPanel3, java.awt.BorderLayout.NORTH);
        validStatusLabel.setFont(new Font("Dialog", Font.PLAIN, 12));
        validStatusLabel.setForeground(Color.RED);
        validStatusLabel.setHorizontalAlignment(SwingConstants.CENTER);

        jPanel1.add(validStatusLabel, java.awt.BorderLayout.SOUTH);

        // for XML viewing
        targetLabel2.setFont(new Font("Dialog", 1, 14));
        targetLabel2.setForeground((Color) UIManager.getDefaults().get("CheckBoxMenuItem.acceleratorForeground"));
        targetLabel2.setHorizontalAlignment(SwingConstants.CENTER);
        targetLabel2.setText("Schema");
        targetLabel2.setBorder(new EtchedBorder());
        validStatusLabel2.setFont(new Font("Dialog", Font.PLAIN, 12));
        validStatusLabel2.setForeground(Color.RED);
        validStatusLabel2.setHorizontalAlignment(SwingConstants.CENTER);

        jSplitPane1.setRightComponent(jPanel1);

        jPanel2.setLayout(new java.awt.BorderLayout());

        jScrollPane1.setViewportView(tree);

        jPanel2.add(jScrollPane1, java.awt.BorderLayout.CENTER);

        jSplitPane1.setLeftComponent(jPanel2);


        //========================================================
        // actions
        //========================================================
        addCube = new AbstractAction("Add cube") {
            public void actionPerformed(ActionEvent e) {
                addCube(e);
            }
        };
        addDimension = new AbstractAction("Add Dimension") {
            public void actionPerformed(ActionEvent e) {
                addDimension(e);
            }
        };
        addDimensionUsage = new AbstractAction("Add Dimension Usage") {
            public void actionPerformed(ActionEvent e) {
                addDimensionUsage(e);
            }
        };
        addHierarchy = new AbstractAction("Add Hierarchy") {
            public void actionPerformed(ActionEvent e) {
                addHierarchy(e);
            }
        };
        addNamedSet = new AbstractAction("Add Named Set") {
            public void actionPerformed(ActionEvent e) {
                addNamedSet(e);
            }
        };
        addMeasure = new AbstractAction("Add Measure") {
            public void actionPerformed(ActionEvent e) {
                addMeasure(e);
            }
        };
        addCalculatedMember = new AbstractAction("Add Calculated Measure") {
            public void actionPerformed(ActionEvent e) {
                addCalculatedMember(e);
            }
        };
        addUserDefinedFunction = new AbstractAction("Add User Defined Function") {
            public void actionPerformed(ActionEvent e) {
                addUserDefinedFunction(e);
            }
        };
        addRole = new AbstractAction("Add Role") {
            public void actionPerformed(ActionEvent e) {
                addRole(e);
            }
        };
        addSchemaGrant = new AbstractAction("Add Schema Grant") {
            public void actionPerformed(ActionEvent e) {
                addSchemaGrant(e);
            }
        };
        addCubeGrant = new AbstractAction("Add Cube Grant") {
            public void actionPerformed(ActionEvent e) {
                addCubeGrant(e);
            }
        };
        addDimensionGrant = new AbstractAction("Add Dimension Grant") {
            public void actionPerformed(ActionEvent e) {
                addDimensionGrant(e);
            }
        };
        addHierarchyGrant = new AbstractAction("Add Hierarchy Grant") {
            public void actionPerformed(ActionEvent e) {
                addHierarchyGrant(e);
            }
        };
        addMemberGrant = new AbstractAction("Add Member Grant") {
            public void actionPerformed(ActionEvent e) {
                addMemberGrant(e);
            }
        };

        addLevel = new AbstractAction("Add Level") {
            public void actionPerformed(ActionEvent e) {
                addLevel(e);
            }
        };
        addClosure = new AbstractAction("Add Closure") {
            public void actionPerformed(ActionEvent e) {
                addClosure(e);
            }
        };
        addKeyExp = new AbstractAction("Add Key Expression") {
            public void actionPerformed(ActionEvent e) {
                addKeyExp(e);
            }
        };
        addNameExp = new AbstractAction("Add Name Expression") {
            public void actionPerformed(ActionEvent e) {
                addNameExp(e);
            }
        };
        addOrdinalExp = new AbstractAction("Add Ordinal Expression") {
            public void actionPerformed(ActionEvent e) {
                addOrdinalExp(e);
            }
        };
        addParentExp = new AbstractAction("Add Parent Expression") {
            public void actionPerformed(ActionEvent e) {
                addParentExp(e);
            }
        };
        addMeasureExp = new AbstractAction("Add Measure Expression") {
            public void actionPerformed(ActionEvent e) {
                addMeasureExp(e);
            }
        };
        addSQL = new AbstractAction("Add SQL") {
            public void actionPerformed(ActionEvent e) {
                addSQL(e);
            }
        };
        addRelation = new AbstractAction("Add Relation") {
            public void actionPerformed(ActionEvent e) {
                addRelation(e);
            }
        };
        addProperty = new AbstractAction("Add Property") {
            public void actionPerformed(ActionEvent e) {
                addProperty(e);
            }
        };

        addVirtualCube = new AbstractAction("Add Virtual Cube") {
            public void actionPerformed(ActionEvent e) {
                addVirtualCube(e);
            }
        };
        addVirtualCubeDimension = new AbstractAction("Add Virtual Cube Dimension") {
            public void actionPerformed(ActionEvent e) {
                addVirtualCubeDimension(e);
            }
        };
        addVirtualCubeMeasure = new AbstractAction("Add Virtual Cube Measure") {
            public void actionPerformed(ActionEvent e) {
                addVirtualCubeMeasure(e);
            }
        };

        addAggPattern = new AbstractAction("Add Aggregate Pattern") {
            public void actionPerformed(ActionEvent e) {
                addAggPattern(e);
            }
        };
        addAggExclude = new AbstractAction("Add Aggregate Exclude Table") {
            public void actionPerformed(ActionEvent e) {
                addAggExclude(e);
            }
        };
        addAggName = new AbstractAction("Add Aggregate Name") {
            public void actionPerformed(ActionEvent e) {
                addAggName(e);
            }
        };
        addAggIgnoreColumn = new AbstractAction("Add Aggregate Ignore Column") {
            public void actionPerformed(ActionEvent e) {
                addAggIgnoreColumn(e);
            }
        };
        addAggForeignKey = new AbstractAction("Add Aggregate Foreign Key") {
            public void actionPerformed(ActionEvent e) {
                addAggForeignKey(e);
            }
        };
        addAggMeasure = new AbstractAction("Add Aggregate Measure") {
            public void actionPerformed(ActionEvent e) {
                addAggMeasure(e);
            }
        };
        addAggLevel = new AbstractAction("Add Aggregate Level") {
            public void actionPerformed(ActionEvent e) {
                addAggLevel(e);
            }
        };
        addAggFactCount = new AbstractAction("Add Aggregate Fact Count") {
            public void actionPerformed(ActionEvent e) {
                addAggFactCount(e);
            }
        };

        delete = new AbstractAction("Delete") {
            public void actionPerformed(ActionEvent e) {
                delete(e);
            }
        };

        editMode = new AbstractAction("EditMode") {
            public void actionPerformed(ActionEvent e) {
                editMode(e);
            }
        };

        //========================================================
        // toolbar buttons
        //========================================================
        addCubeButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addCube"))));
        addCubeButton.setToolTipText("Add Cube");
        addCubeButton.addActionListener(addCube);

        addDimensionButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addDimension"))));
        addDimensionButton.setToolTipText("Add Dimension");
        addDimensionButton.addActionListener(addDimension);

        addDimensionUsageButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addDimensionUsage"))));
        addDimensionUsageButton.setToolTipText("Add Dimension Usage");
        addDimensionUsageButton.addActionListener(addDimensionUsage);

        addHierarchyButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addHierarchy"))));
        addHierarchyButton.setToolTipText("Add Hierarchy");
        addHierarchyButton.addActionListener(addHierarchy);

        addNamedSetButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addNamedSet"))));
        addNamedSetButton.setToolTipText("Add Named Set");
        addNamedSetButton.addActionListener(addNamedSet);

        addUserDefinedFunctionButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addUserDefinedFunction"))));
        addUserDefinedFunctionButton.setToolTipText("Add User defined Function");
        addUserDefinedFunctionButton.addActionListener(addUserDefinedFunction);

        addCalculatedMemberButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addCalculatedMember"))));
        addCalculatedMemberButton.setToolTipText("Add Calculated Member");
        addCalculatedMemberButton.addActionListener(addCalculatedMember);

        addMeasureButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addMeasure"))));
        addMeasureButton.setToolTipText("Add Measure");
        addMeasureButton.addActionListener(addMeasure);

        addLevelButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addLevel"))));
        addLevelButton.setToolTipText("Add Level");
        addLevelButton.addActionListener(addLevel);

        addPropertyButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addProperty"))));
        addPropertyButton.setToolTipText("Add Property");
        addPropertyButton.addActionListener(addProperty);

        addVirtualCubeButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addVirtualCube"))));
        addVirtualCubeButton.setToolTipText("Add Virtual Cube");
        addVirtualCubeButton.addActionListener(addVirtualCube);

        addVirtualCubeDimensionButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addVirtualCubeDimension"))));
        addVirtualCubeDimensionButton.setToolTipText("Add Virtual Dimension");
        addVirtualCubeDimensionButton.addActionListener(addVirtualCubeDimension);

        addVirtualCubeMeasureButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addVirtualCubeMeasure"))));
        addVirtualCubeMeasureButton.setToolTipText("Add Virtual Measure");
        addVirtualCubeMeasureButton.addActionListener(addVirtualCubeMeasure);

        addRoleButton.setIcon(new ImageIcon(myClassLoader.getResource(resources.getString("addRole"))));
        addRoleButton.setToolTipText("Add Role");
        addRoleButton.addActionListener(addRole);

        cutButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/Cut24.gif")));
        cutButton.setToolTipText("Cut");

        copyButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/Copy24.gif")));
        copyButton.setToolTipText("Copy");

        pasteButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/Paste24.gif")));
        pasteButton.setToolTipText("Paste");

        deleteButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/Delete24.gif")));
        deleteButton.setToolTipText("Delete");
        deleteButton.addActionListener(delete);

        editModeButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/general/Edit24.gif")));
        editModeButton.setToolTipText("View XML");
        editModeButton.addActionListener(editMode);

        databaseLabel.setIcon(new javax.swing.ImageIcon(getClass().getResource("/toolbarButtonGraphics/development/Server24.gif")));


        jToolBar1.add(addCubeButton);
        jToolBar1.add(addDimensionButton);
        jToolBar1.add(addDimensionUsageButton);
        jToolBar1.add(addHierarchyButton);
        jToolBar1.add(addNamedSetButton);
        jToolBar1.add(addUserDefinedFunctionButton);
        jToolBar1.add(addCalculatedMemberButton);
        jToolBar1.add(addMeasureButton);
        jToolBar1.add(addLevelButton);
        jToolBar1.add(addPropertyButton);
        jToolBar1.addSeparator();
        jToolBar1.add(addVirtualCubeButton);
        jToolBar1.add(addVirtualCubeDimensionButton);
        jToolBar1.add(addVirtualCubeMeasureButton);
        jToolBar1.addSeparator();
        jToolBar1.add(addRoleButton);
        jToolBar1.addSeparator();

        jToolBar1.add(cutButton);
        jToolBar1.add(copyButton);
        jToolBar1.add(pasteButton);
        jToolBar1.addSeparator();
        jToolBar1.add(deleteButton);
        jToolBar1.addSeparator();
        jToolBar1.add(editModeButton);

        //========================================================
        // popup menu
        //========================================================
        jPopupMenu = new JPopupMenu();

        //========================================================
        // tree mouse listener
        //========================================================
        tree.addMouseListener(new PopupTrigger());
        tree.addKeyListener(new KeyAdapter() {
            public void keyPressed(KeyEvent e) {
                    /*
                    keytext=Delete
                    keycode=127
                    keytext=NumPad .
                    keycode=110
                     */
                int kcode = e.getKeyCode();
                if (kcode == 127 || kcode == 110) {
                    delete(e);
                }
            }
        });


        // add footer for connected database
        footer.setLayout(new java.awt.BorderLayout());
        footer.add(databaseLabel, java.awt.BorderLayout.CENTER);

        //========================================================
        // jpanel
        //========================================================
        this.add(jSplitPane1, java.awt.BorderLayout.CENTER);
        this.add(jToolBar1, java.awt.BorderLayout.NORTH);
        this.add(footer, java.awt.BorderLayout.SOUTH);

        updater = new JTreeUpdater(tree);

    }

    protected void arrowButtonUpAction(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        if (tpath != null) {
            TreePath parentpath = tpath.getParentPath();
            if (parentpath != null) {
                tree.setSelectionPath(parentpath);
                refreshTree(parentpath);
            }
        }
    }

    protected void arrowButtonDownAction(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        if (tpath != null) {
            Object current = tpath.getLastPathComponent();
            Object child = tree.getModel().getChild(current, 0);
            if (child != null) {
                Object[] treeObjs = new Object[30];
                treeObjs[0] = child;
                treeObjs[1] = current;
                // traverse upward through tree, saving parent nodes
                TreePath parentpath;
                parentpath = tpath.getParentPath();
                int objCnt = 2;
                while (parentpath != null) {
                    Object po = parentpath.getLastPathComponent();
                    treeObjs[objCnt] = po;
                    objCnt += 1;
                    parentpath = parentpath.getParentPath();
                }
                // reverse the array so that schema is first, then the children
                Object[] nodes = new Object[objCnt];
                int loopCnt = objCnt - 1;
                for (int j = 0; j < objCnt; j++) {
                    nodes[j] = treeObjs[loopCnt];
                    loopCnt --;
                }
                TreePath childPath = new TreePath(nodes);
                tree.setSelectionPath(childPath);
                refreshTree(childPath);
            }
        }
    }

    /**
     * Several methods are called, e.g. editCellAt,  to get the focus set in the
     * value column of the specified row.  The attribute column has the parameter
     * name and should not receive focus.
     */
    protected void setTableCellFocus(int row) {
        propertyTable.editCellAt(row, 1);
        TableCellEditor editor = propertyTable.getCellEditor(row, 1);
        Component comp = editor.getTableCellEditorComponent(propertyTable,
                propertyTable.getValueAt(row, 1), true, row, 1);
    }

    /**
     * @param evt
     */
    protected void addCube(ActionEvent evt) {
        MondrianGuiDef.Schema schema = (MondrianGuiDef.Schema) tree.getModel().getRoot();
        MondrianGuiDef.Cube cube = new MondrianGuiDef.Cube();

        cube.name = "";// get unique name //"New Cube " + schema.cubes.length;

        cube.dimensions = new MondrianGuiDef.Dimension[0];
        cube.measures = new MondrianGuiDef.Measure[0];
        MondrianGuiDef.Table cfact = new MondrianGuiDef.Table("","Table","");
        cfact.aggExcludes = new MondrianGuiDef.AggExclude[0];
        cfact.aggTables = new MondrianGuiDef.AggTable[0];

        cube.fact = cfact;
        cube.calculatedMembers = new MondrianGuiDef.CalculatedMember[0];
        cube.namedSets = new MondrianGuiDef.NamedSet[0];

        //add cube to schema
        cube.name = getNewName("New Cube ", schema.cubes);
        cube.cache = Boolean.TRUE;
        cube.enabled = Boolean.TRUE;
        NodeDef[] temp = schema.cubes;
        schema.cubes = new MondrianGuiDef.Cube[temp.length + 1];
        for (int _i = 0; _i < temp.length; _i++) {
            schema.cubes[_i] = (MondrianGuiDef.Cube) temp[_i];
        }
        schema.cubes[schema.cubes.length - 1] = cube;

        tree.setSelectionPath((new TreePath(model.getRoot())).pathByAddingChild(cube));
        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addRole(ActionEvent evt) {
        MondrianGuiDef.Schema schema = (MondrianGuiDef.Schema) tree.getModel().getRoot();
        MondrianGuiDef.Role role = new MondrianGuiDef.Role();

        role.name = "";// get unique name //"New Cube " + schema.cubes.length;

        role.schemaGrants = new MondrianGuiDef.SchemaGrant[0];

        //add cube to schema
        role.name = getNewName("New Role ", schema.roles);
        NodeDef[] temp = schema.roles;
        schema.roles = new MondrianGuiDef.Role[temp.length + 1];
        for (int _i = 0; _i < temp.length; _i++) {
            schema.roles[_i] = (MondrianGuiDef.Role) temp[_i];
        }
        schema.roles[schema.roles.length - 1] = role;

        tree.setSelectionPath((new TreePath(model.getRoot())).pathByAddingChild(role));
        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addVirtualCube(ActionEvent evt) {
        MondrianGuiDef.Schema schema = (MondrianGuiDef.Schema) tree.getModel().getRoot();
        MondrianGuiDef.VirtualCube cube = new MondrianGuiDef.VirtualCube();

        cube.name = "";// get unique name //"New Cube " + schema.cubes.length;

        cube.dimensions = new MondrianGuiDef.VirtualCubeDimension[0];
        cube.measures = new MondrianGuiDef.VirtualCubeMeasure[0];
        cube.calculatedMembers = new MondrianGuiDef.CalculatedMember[0];
        cube.enabled = Boolean.TRUE;

        //add cube to schema
        cube.name = getNewName("New Virtual Cube ", schema.virtualCubes);
        NodeDef[] temp = schema.virtualCubes;
        schema.virtualCubes = new MondrianGuiDef.VirtualCube[temp.length + 1];
        for (int _i = 0; _i < temp.length; _i++) {
            schema.virtualCubes[_i] = (MondrianGuiDef.VirtualCube) temp[_i];
        }
        schema.virtualCubes[schema.virtualCubes.length - 1] = cube;

        tree.setSelectionPath((new TreePath(model.getRoot())).pathByAddingChild(cube));
        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addUserDefinedFunction(ActionEvent evt) {
        MondrianGuiDef.Schema schema = (MondrianGuiDef.Schema) tree.getModel().getRoot();
        MondrianGuiDef.UserDefinedFunction udf = new MondrianGuiDef.UserDefinedFunction();
        udf.name = "";// get unique name //"New Udf " + schema.userDefinedFunctions.length;

        //add cube to schema
        udf.name = getNewName("New Udf ", schema.userDefinedFunctions);
        NodeDef[] temp = schema.userDefinedFunctions;
        schema.userDefinedFunctions = new MondrianGuiDef.UserDefinedFunction[temp.length + 1];
        for (int _i = 0; _i < temp.length; _i++) {
            schema.userDefinedFunctions[_i] = (MondrianGuiDef.UserDefinedFunction) temp[_i];
        }
        schema.userDefinedFunctions[schema.userDefinedFunctions.length - 1] = udf;
        tree.setSelectionPath((new TreePath(model.getRoot())).pathByAddingChild(udf));
        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    /**
     * Updates the tree display after an Add / Delete operation.
     */
    private void refreshTree(TreePath path) {
        setDirty(true);
        if (! dirtyFlag) {
            setDirtyFlag(true);   // dirty indication shown on title
            setTitle();
        }
        updater.update();
        tree.scrollPathToVisible(path);
    }

    /**
     * @param evt
     */
    protected void addMeasure(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Cube) {
                    path = tpath.getPathComponent(parentIndex);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.Cube)) {
            JOptionPane.showMessageDialog(this, "Cube not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }


        MondrianGuiDef.Cube cube = (MondrianGuiDef.Cube) path;

        MondrianGuiDef.Measure measure = new MondrianGuiDef.Measure();
        measure.name = ""; // get unique name //"New Measure " + cube.measures.length;

        //add cube to schema
        measure.name = getNewName("New Measure ", cube.measures);
        measure.visible = Boolean.TRUE;
        NodeDef[] temp = cube.measures;
        cube.measures = new MondrianGuiDef.Measure[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            cube.measures[i] = (MondrianGuiDef.Measure) temp[i];}

        cube.measures[cube.measures.length - 1] = measure;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(measure));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addAggPattern(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Table) {
                    if (((parentIndex-1) >=0) && (tpath.getPathComponent(parentIndex-1) instanceof MondrianGuiDef.Cube)) {
                        path = tpath.getPathComponent(parentIndex);
                        break;
                    }
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.Table)) {
            JOptionPane.showMessageDialog(this, "Cube Fact Table not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }


        MondrianGuiDef.Table factTable = (MondrianGuiDef.Table) path;

        MondrianGuiDef.AggPattern aggname = new MondrianGuiDef.AggPattern();
        aggname.pattern = ""; // get unique name //"New Measure " + cube.measures.length;

        //add cube to schema
        aggname.ignorecase = Boolean.TRUE;
        aggname.factcount = null;
        aggname.ignoreColumns = new MondrianGuiDef.AggIgnoreColumn[0];
        aggname.foreignKeys = new MondrianGuiDef.AggForeignKey[0];
        aggname.measures = new MondrianGuiDef.AggMeasure[0];
        aggname.levels = new MondrianGuiDef.AggLevel[0];
        aggname.excludes = new MondrianGuiDef.AggExclude[0];

        NodeDef[] temp = factTable.aggTables;
        factTable.aggTables = new MondrianGuiDef.AggTable[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            factTable.aggTables[i] = (MondrianGuiDef.AggTable) temp[i];}

        factTable.aggTables[factTable.aggTables.length - 1] = aggname;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(aggname));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addAggName(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Table) {
                    if (((parentIndex-1) >=0) && (tpath.getPathComponent(parentIndex-1) instanceof MondrianGuiDef.Cube)) {
                        path = tpath.getPathComponent(parentIndex);
                        break;
                    }
                }
            }

        }
        if (!(path instanceof MondrianGuiDef.Table)) {
            JOptionPane.showMessageDialog(this, "Table not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.Table factTable = (MondrianGuiDef.Table) path;

        MondrianGuiDef.AggName aggname = new MondrianGuiDef.AggName();
        aggname.name = ""; // get unique name //"New Measure " + cube.measures.length;

        //add cube to schema
        aggname.ignorecase = Boolean.TRUE;
        aggname.factcount = null;
        aggname.ignoreColumns = new MondrianGuiDef.AggIgnoreColumn[0];
        aggname.foreignKeys = new MondrianGuiDef.AggForeignKey[0];
        aggname.measures = new MondrianGuiDef.AggMeasure[0];
        aggname.levels = new MondrianGuiDef.AggLevel[0];


        NodeDef[] temp = factTable.aggTables;
        factTable.aggTables = new MondrianGuiDef.AggTable[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            factTable.aggTables[i] = (MondrianGuiDef.AggTable) temp[i];}

        factTable.aggTables[factTable.aggTables.length - 1] = aggname;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(aggname));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addAggExclude(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                // aggexcludes can be added to cube fact table or
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Table) {
                    if (((parentIndex-1) >=0) && (tpath.getPathComponent(parentIndex-1) instanceof MondrianGuiDef.Cube)) {
                        path = tpath.getPathComponent(parentIndex);
                        break;
                    }
                } else if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.AggPattern) {
                    // aggexcludes can also be added to aggregate patterns
                    path = tpath.getPathComponent(parentIndex);
                    break;

                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.Table || path instanceof MondrianGuiDef.AggPattern)) {
            JOptionPane.showMessageDialog(this, "Cube Fact Table or Aggregate Pattern not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.AggExclude aggexclude = new MondrianGuiDef.AggExclude();
        aggexclude.pattern = ""; // get unique name //"New Measure " + cube.measures.length;

        aggexclude.ignorecase = Boolean.TRUE;

        if (path instanceof MondrianGuiDef.Table) {
            MondrianGuiDef.Table parent = (MondrianGuiDef.Table) path;  // fact table

            NodeDef[] temp = parent.aggExcludes;
            parent.aggExcludes = new MondrianGuiDef.AggExclude[temp.length + 1];
            for (int i = 0; i < temp.length; i++) {
                parent.aggExcludes[i] = (MondrianGuiDef.AggExclude) temp[i];}

            parent.aggExcludes[parent.aggExcludes.length - 1] = aggexclude;
        } else {
            MondrianGuiDef.AggPattern parent = (MondrianGuiDef.AggPattern) path;  // aggpattern
            NodeDef[] temp = parent.excludes;
            parent.excludes = new MondrianGuiDef.AggExclude[temp.length + 1];
            for (int i = 0; i < temp.length; i++) {
                parent.excludes[i] = (MondrianGuiDef.AggExclude) temp[i];}

            parent.excludes[parent.excludes.length - 1] = aggexclude;
        }

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(aggexclude));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addAggIgnoreColumn(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.AggTable) {
                    path = tpath.getPathComponent(parentIndex);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.AggTable)) {
            JOptionPane.showMessageDialog(this, "Aggregate Table not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }


        MondrianGuiDef.AggTable aggTable = (MondrianGuiDef.AggTable) path;

        MondrianGuiDef.AggIgnoreColumn aggicol = new MondrianGuiDef.AggIgnoreColumn();
        aggicol.column = "";

        NodeDef[] temp = aggTable.ignoreColumns;
        aggTable.ignoreColumns = new MondrianGuiDef.AggIgnoreColumn[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            aggTable.ignoreColumns[i] = (MondrianGuiDef.AggIgnoreColumn) temp[i];}

        aggTable.ignoreColumns[aggTable.ignoreColumns.length - 1] = aggicol;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(aggicol));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addAggForeignKey(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.AggTable) {
                    path = tpath.getPathComponent(parentIndex);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.AggTable)) {
            JOptionPane.showMessageDialog(this, "Aggregate Table not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }


        MondrianGuiDef.AggTable aggTable = (MondrianGuiDef.AggTable) path;

        MondrianGuiDef.AggForeignKey aggfkey = new MondrianGuiDef.AggForeignKey();

        NodeDef[] temp = aggTable.foreignKeys;
        aggTable.foreignKeys = new MondrianGuiDef.AggForeignKey[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            aggTable.foreignKeys[i] = (MondrianGuiDef.AggForeignKey) temp[i];}

        aggTable.foreignKeys[aggTable.foreignKeys.length - 1] = aggfkey;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(aggfkey));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addAggMeasure(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.AggTable) {
                    path = tpath.getPathComponent(parentIndex);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.AggTable)) {
            JOptionPane.showMessageDialog(this, "Aggregate Table not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }


        MondrianGuiDef.AggTable aggTable = (MondrianGuiDef.AggTable) path;

        MondrianGuiDef.AggMeasure aggmeasure = new MondrianGuiDef.AggMeasure();

        NodeDef[] temp = aggTable.measures;
        aggTable.measures = new MondrianGuiDef.AggMeasure[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            aggTable.measures[i] = (MondrianGuiDef.AggMeasure) temp[i];}

        aggTable.measures[aggTable.measures.length - 1] = aggmeasure;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(aggmeasure));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addAggLevel(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.AggTable) {
                    path = tpath.getPathComponent(parentIndex);
                    break;
                }
            }

        }
        if (!(path instanceof MondrianGuiDef.AggTable)) {
            JOptionPane.showMessageDialog(this, "Aggregate Table not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }


        MondrianGuiDef.AggTable aggTable = (MondrianGuiDef.AggTable) path;

        MondrianGuiDef.AggLevel agglevel = new MondrianGuiDef.AggLevel();

        NodeDef[] temp = aggTable.levels;
        aggTable.levels = new MondrianGuiDef.AggLevel[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            aggTable.levels[i] = (MondrianGuiDef.AggLevel) temp[i];}

        aggTable.levels[aggTable.levels.length - 1] = agglevel;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(agglevel));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addAggFactCount(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.AggTable) {
                    path = tpath.getPathComponent(parentIndex);
                    break;
                }
            }

        }
        if ( ! ((path instanceof MondrianGuiDef.AggName) ||
                (path instanceof MondrianGuiDef.AggPattern) )) {
            JOptionPane.showMessageDialog(this, "Aggregate Table or Aggregate Pattern not selected.",
                    alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.AggFactCount aggFactCount = new MondrianGuiDef.AggFactCount();
        MondrianGuiDef.AggTable aggName = null;
        MondrianGuiDef.AggPattern aggPattern = null;
        if (path instanceof MondrianGuiDef.AggName) {
            aggName = (MondrianGuiDef.AggName) path;
            aggName.factcount = new MondrianGuiDef.AggFactCount();
        } else {
            aggPattern = (MondrianGuiDef.AggPattern) path;
            aggPattern.factcount = new MondrianGuiDef.AggFactCount();
        }

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(aggFactCount));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addVirtualCubeMeasure(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.VirtualCube) {
                    //System.out.println("==== path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
                    path = tpath.getPathComponent(parentIndex);
                    //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.VirtualCube)) {
            JOptionPane.showMessageDialog(this, "Virtual Cube not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }


        MondrianGuiDef.VirtualCube cube = (MondrianGuiDef.VirtualCube) path;

        MondrianGuiDef.VirtualCubeMeasure measure = new MondrianGuiDef.VirtualCubeMeasure();
        measure.name = ""; // get unique name //"New Measure " + cube.measures.length;

        //add cube to schema
        measure.name = getNewName("New Virtual Measure ", cube.measures);
        measure.visible = Boolean.TRUE; // default true

        NodeDef[] temp = cube.measures;
        cube.measures = new MondrianGuiDef.VirtualCubeMeasure[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            cube.measures[i] = (MondrianGuiDef.VirtualCubeMeasure) temp[i];}

        cube.measures[cube.measures.length - 1] = measure;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(measure));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addCalculatedMember(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        Object path = null;
        int parentIndex = -1;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if ( (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Cube) ||
                        (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.VirtualCube)   ) {
                    //System.out.println("==== path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
                    path = tpath.getPathComponent(parentIndex);
                    //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
                    break;
                }
            }

        }
        if ( ! ((path instanceof MondrianGuiDef.Cube) || (path instanceof MondrianGuiDef.VirtualCube) )) {
            JOptionPane.showMessageDialog(this, "Cube or Virtual Cube not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.Cube cube = null;
        MondrianGuiDef.VirtualCube vcube = null;

        if (path instanceof MondrianGuiDef.Cube) {
            cube = (MondrianGuiDef.Cube) path;
        } else {
            vcube = (MondrianGuiDef.VirtualCube) path;
        }

        MondrianGuiDef.CalculatedMember calcmember = new MondrianGuiDef.CalculatedMember();
        calcmember.name = ""; // get unique name //"New Calculated Measure " + cube.calculatedMembers.length;
        calcmember.dimension = "Measures";
        calcmember.visible = Boolean.TRUE;  // default value
        calcmember.formatString = "";
        calcmember.formula="";
        calcmember.formulaElement = new MondrianGuiDef.Formula();
        calcmember.memberProperties = new MondrianGuiDef.CalculatedMemberProperty[0];

        //add cube to schema
        if (cube != null) {
            calcmember.name = getNewName("New Calculated Measure ", cube.calculatedMembers);
            NodeDef[] temp = cube.calculatedMembers;
            cube.calculatedMembers = new MondrianGuiDef.CalculatedMember[temp.length + 1];
            for (int i = 0; i < temp.length; i++) {
                cube.calculatedMembers[i] = (MondrianGuiDef.CalculatedMember) temp[i];}

            cube.calculatedMembers[cube.calculatedMembers.length - 1] = calcmember;
        } else {
            calcmember.name = getNewName("New Calculated Measure ", vcube.calculatedMembers);
            NodeDef[] temp = vcube.calculatedMembers;
            vcube.calculatedMembers = new MondrianGuiDef.CalculatedMember[temp.length + 1];
            for (int i = 0; i < temp.length; i++) {
                vcube.calculatedMembers[i] = (MondrianGuiDef.CalculatedMember) temp[i];}

            vcube.calculatedMembers[vcube.calculatedMembers.length - 1] = calcmember;

        }

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(calcmember));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
}

    protected boolean editMode(EventObject evt) {
        editModeXML = ! isEditModeXML();    // toggle edit mode between xml or properties table form
        editModeButton.setSelected(isEditModeXML());
        if (isEditModeXML()) {
            jSplitPane1.setRightComponent(jPanelXML);
        } else {
            jSplitPane1.setRightComponent(jPanel1);
        }
        // update the workbench view menu
        Component o = parentIFrame.getDesktopPane().getParent();
        while (o != null) {
            if (o.getClass() == Workbench.class) {
                ((Workbench) o).getViewXMLMenuItem().setSelected(editModeXML);
                break;
                //System.out.println("desktpo parent class ==="+o.getClass());
            }
            o= o.getParent();
        }
        return isEditModeXML();
    }

    protected void delete(EventObject evt) {
        // delete the selected  schema object
        TreePath tpath = tree.getSelectionPath();
        if (tpath == null) {
            JOptionPane.showMessageDialog(this, "Select an object in Schema to delete.", alert, JOptionPane.WARNING_MESSAGE);
            return ;
        }
        Object child = tpath.getLastPathComponent(); // to be deleted
        Object nextSibling = null, prevSibling = null;

        Object parent = null;
        Object grandparent = null;
        boolean grandparentAsSibling = false;

        for(int i=tpath.getPathCount()-1-1; i>=0;i--) {
            parent = tpath.getPathComponent(i);   // get parent path
            if (tpath.getPathCount()-3 > 0) {
                grandparent = tpath.getPathComponent(i-1);   // get parent path
            }
            //System.out.println("path count=="+tpath.getPathCount());
            //System.out.println("==== Parent path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
            //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
            break;
        }
        if (parent == null) {
            JOptionPane.showMessageDialog(this, "Schema object cannot be deleted.", alert, JOptionPane.WARNING_MESSAGE);
            return ;
        }

        boolean tofind = true;
        /*
        if (parent instanceof MondrianGuiDef.Cube) {
            MondrianGuiDef.Cube c = (MondrianGuiDef.Cube)parent;
            if (child instanceof MondrianGuiDef.CubeDimension) {
                NodeDef[] temp = c.dimensions;
                c.dimensions = new MondrianGuiDef.CubeDimension[temp.length - 1];
                tofind = true;
                for (int i=0, j=0; i < temp.length; i++) {
                    if (tofind && temp[i].equals(child)) {
                        // check equality of parent attributes
                        MondrianGuiDef.CubeDimension match = (MondrianGuiDef.CubeDimension) temp[i];
                        MondrianGuiDef.CubeDimension d = (MondrianGuiDef.CubeDimension) child;
                        if ( ((match.name== null && d.name==null) || ( match.name != null && match.name.equals(d.name) )) &&
                                ((match.caption== null && d.caption==null) || ( match.caption != null && match.caption.equals(d.name) )) &&
                                ((match.foreignKey== null && d.foreignKey==null) || ( match.foreignKey != null && match.name.equals(d.foreignKey) )) ) {
                            tofind = false;
                            if (i+1 < temp.length) {
                                nextSibling = temp[i+1];
                            }
                            if(i-1 >= 0) {
                                prevSibling = temp[i-1];
                            }
                            continue;
                        }
                    }
                    c.dimensions[j++] = (MondrianGuiDef.CubeDimension) temp[i];
                }*/
        //=============================
        Field[] fs = parent.getClass().getFields();
        for (int i = 0; i < fs.length; i++) {
            if (fs[i].getType().isArray() && (fs[i].getType().getComponentType().isInstance(child) )) {
                try {
                    Object parentArr = fs[i].get(parent); // get the parent's array of child objects
                    int parentArrLen = Array.getLength(parentArr);
                    Object newArr = Array.newInstance(fs[i].getType().getComponentType(), parentArrLen-1);
                    tofind = true;
                    for (int k=0, m=0; k < parentArrLen; k++) {
                        Object match = Array.get(parentArr, k);

                        if (tofind && match.equals(child)) {
                            if (child instanceof MondrianGuiDef.CubeDimension) {
                                // check equality of parent class attributes for a special case when child is an object of CubeDimensions
                                MondrianGuiDef.CubeDimension matchDim = (MondrianGuiDef.CubeDimension) match;
                                MondrianGuiDef.CubeDimension childDim = (MondrianGuiDef.CubeDimension) child;
                                /*
                                System.out.println("matchDim.name="+matchDim.name);
                                System.out.println("childDim.name="+childDim.name);
                                System.out.println("matchDim.caption="+matchDim.caption);
                                System.out.println("childDim.caption="+childDim.caption);
                                System.out.println("matchDim.foreignKey="+matchDim.foreignKey);
                                System.out.println("childDim.foreignKey="+childDim.foreignKey);
                                System.out.println("1st="+((matchDim.name== null && childDim.name==null) || ( matchDim.name != null && matchDim.name.equals(childDim.name) )));
                                System.out.println("2nd="+((matchDim.caption== null && childDim.caption==null) || ( matchDim.caption != null && matchDim.caption.equals(childDim.caption) )));
                                System.out.println("3rd="+((matchDim.foreignKey== null && childDim.foreignKey==null) || ( matchDim.foreignKey != null && matchDim.foreignKey.equals(childDim.foreignKey) )));
                                 */
                                if ( ((matchDim.name== null && childDim.name==null) || ( matchDim.name != null && matchDim.name.equals(childDim.name) )) &&
                                        ((matchDim.caption== null && childDim.caption==null) || ( matchDim.caption != null && matchDim.caption.equals(childDim.caption) )) &&
                                        ((matchDim.foreignKey== null && childDim.foreignKey==null) || ( matchDim.foreignKey != null && matchDim.foreignKey.equals(childDim.foreignKey) )) ) {
                                    tofind = false;
                                    if (k+1 < parentArrLen) {
                                        nextSibling = Array.get(parentArr, k+1);
                                    }
                                    if(k-1 >= 0) {
                                        prevSibling = Array.get(parentArr, k-1);
                                    }
                                    continue;
                                }
                            } else {
                                // other cases require no such check
                                tofind = false;
                                if (k+1 < parentArrLen) {
                                    nextSibling = Array.get(parentArr, k+1);
                                }
                                if(k-1 >= 0) {
                                    prevSibling = Array.get(parentArr, k-1);
                                }
                                continue;

                            }
                        }
                        Array.set(newArr, m++, match);
                    }
                    // after deletion check before the saving the new array in parent
                    // check for min 1 SQL object(child)  in (parent) expression  for (grandparent) level
                    // if 1 or more, save the newarray in parent, otherwise delete parent from grandparent
                    if ( (child instanceof MondrianGuiDef.SQL) &&
                            (parent instanceof MondrianGuiDef.ExpressionView) &&
                            (Array.getLength(newArr) < 1) ) {
                        if (parent instanceof MondrianGuiDef.KeyExpression) {
                            ((MondrianGuiDef.Level)grandparent).keyExp = null;
                        } else if (parent instanceof MondrianGuiDef.NameExpression) {
                            ((MondrianGuiDef.Level)grandparent).nameExp = null;
                        } else if (parent instanceof MondrianGuiDef.OrdinalExpression) {
                            ((MondrianGuiDef.Level)grandparent).ordinalExp = null;
                        } else if (parent instanceof MondrianGuiDef.ParentExpression) {
                            ((MondrianGuiDef.Level)grandparent).parentExp = null;
                        } else if (parent instanceof MondrianGuiDef.MeasureExpression) {
                            ((MondrianGuiDef.Measure)grandparent).measureExp = null;
                        }
                        grandparentAsSibling = true;
                    } else {
                        fs[i].set(parent, newArr);
                    }

                } catch(Exception ex) {
                    // field not found
                }
                break;
            } else if  ( fs[i].getType().isInstance(child) ) {  // parent's field is an instanceof child object'
                try {
                    if (parent instanceof MondrianGuiDef.Join ||
                        (parent instanceof MondrianGuiDef.Cube && child instanceof MondrianGuiDef.Relation) ||
                        (parent instanceof MondrianGuiDef.Closure && child instanceof MondrianGuiDef.Relation)
                        ) {
                        // do not delete if deleting left or right table of a join
                        // do not delete table of cube or closure
                    } else {
                        if (fs[i].get(parent)==child) {
                            fs[i].set(parent, null);
                            break;
                        }
                    }
                } catch (Exception ex) {
                    // field not found
                }
            }
        }


        // delete the node from set of expended nodes in JTreeUpdater also
        TreeExpansionEvent e = null;
        e = new TreeExpansionEvent(tree, tpath);
        updater.treeCollapsed(e);

        if (nextSibling != null) {
            tree.setSelectionPath(tpath.getParentPath().pathByAddingChild(nextSibling));
        } else if(prevSibling != null) {
            tree.setSelectionPath(tpath.getParentPath().pathByAddingChild(prevSibling));
        } else if (grandparentAsSibling){
            tree.setSelectionPath(tpath.getParentPath().getParentPath());
        } else {
            tree.setSelectionPath(tpath.getParentPath());
        }
        refreshTree(tree.getSelectionPath());
    }

    /**
     * @param evt
     */
    protected void addDimension(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        Object path = null;
        int parentIndex = -1;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0; parentIndex--) {
                //System.out.println("==== path element "+i+" ="+e.getPath().getPathComponent(i).getClass().toString());
                if ( (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Cube) ||
                        (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Schema) ) {
                    path =  tpath.getPathComponent(parentIndex);
                    break;
                    //System.out.println("Cube fact table name ="+((MondrianGuiDef.Table) ((MondrianGuiDef.Cube) e.getPath().getPathComponent(i)).fact).name);
                }
            }
        }

        //Object path = tree.getSelectionPath().getLastPathComponent();
        if ( ! ((path instanceof MondrianGuiDef.Cube) || (path instanceof MondrianGuiDef.Schema) )) {
            JOptionPane.showMessageDialog(this, "Cube or Schema not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.Cube cube = null;
        MondrianGuiDef.Schema schema = null;

        if (path instanceof MondrianGuiDef.Cube) {
            cube = (MondrianGuiDef.Cube) path;
        } else {
            schema = (MondrianGuiDef.Schema) path;
        }

        MondrianGuiDef.Dimension dimension = new MondrianGuiDef.Dimension();
        dimension.name = "";
        dimension.type = "StandardDimension";    // default dimension type
        dimension.hierarchies = new MondrianGuiDef.Hierarchy[1];
        dimension.hierarchies[0] = new MondrianGuiDef.Hierarchy();
        dimension.hierarchies[0].name = "New Hierarchy 0";
        dimension.hierarchies[0].hasAll = true;
        dimension.hierarchies[0].levels = new MondrianGuiDef.Level[0];
        dimension.hierarchies[0].memberReaderParameters = new MondrianGuiDef.MemberReaderParameter[0];
        dimension.hierarchies[0].relation = new MondrianGuiDef.Table("", "Table", "");

        //add cube to schema
        if (cube != null) {
            dimension.name = getNewName("New Dimension ", cube.dimensions);
            NodeDef[] temp = cube.dimensions;
            cube.dimensions = new MondrianGuiDef.CubeDimension[temp.length + 1];
            for (int i = 0; i < temp.length; i++) {
                cube.dimensions[i] = (MondrianGuiDef.CubeDimension) temp[i];
            }
            cube.dimensions[cube.dimensions.length - 1] = dimension;
        } else {
            dimension.name = getNewName("New Dimension ", schema.dimensions);
            NodeDef[] temp = schema.dimensions;
            schema.dimensions = new MondrianGuiDef.Dimension[temp.length + 1];
            for (int i = 0; i < temp.length; i++) {
                schema.dimensions[i] = (MondrianGuiDef.Dimension) temp[i];}

            schema.dimensions[schema.dimensions.length - 1] = dimension;
        }

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(dimension));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }


    protected void addVirtualCubeDimension(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        Object path = null;
        int parentIndex = -1;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0; parentIndex--) {
                //System.out.println("==== path element "+i+" ="+e.getPath().getPathComponent(i).getClass().toString());
                if ( tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.VirtualCube ) {
                    path =  tpath.getPathComponent(parentIndex);
                    break;
                    //System.out.println("Cube fact table name ="+((MondrianGuiDef.Table) ((MondrianGuiDef.Cube) e.getPath().getPathComponent(i)).fact).name);
                }
            }
        }

        //Object path = tree.getSelectionPath().getLastPathComponent();
        if ( ! (path instanceof MondrianGuiDef.VirtualCube  )) {
            JOptionPane.showMessageDialog(this, "Virtual Cube not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.VirtualCube cube = (MondrianGuiDef.VirtualCube) path;;

        MondrianGuiDef.VirtualCubeDimension dimension = new MondrianGuiDef.VirtualCubeDimension();
        dimension.name = "";

        //add cube to schema
        dimension.name = getNewName("New Virtual Dimension ", cube.dimensions);
        NodeDef[] temp = cube.dimensions;
        cube.dimensions = new MondrianGuiDef.VirtualCubeDimension[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            cube.dimensions[i] = (MondrianGuiDef.VirtualCubeDimension) temp[i];
        }
        cube.dimensions[cube.dimensions.length - 1] = dimension;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(dimension));
        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    private String getNewName(String preName, Object[] objs) {
        boolean exists = true;
        //String preName = "New Dimension ";
        String newName="", objName = "";

        if (objs != null) {
            int objNo = objs.length;
            try {
                Field f = objs.getClass().getComponentType().getField("name");
                while(exists) {
                    newName = preName + objNo++;
                    exists = false;
                    for(int i = 0; i < objs.length; i++) {
                        objName  = (String) f.get(objs[i]);
                        if (newName.equals(objName)) {
                            exists=true;
                            break;
                        }
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        } else {
            newName = preName + 0;
        }
        return newName;
    }
    protected void addNamedSet(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0; parentIndex--) {
                //System.out.println("==== path element "+i+" ="+e.getPath().getPathComponent(i).getClass().toString());
                if ( (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Cube) ||
                        (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Schema) ) {
                    path =  tpath.getPathComponent(parentIndex);
                    break;
                    //System.out.println("Cube fact table name ="+((MondrianGuiDef.Table) ((MondrianGuiDef.Cube) e.getPath().getPathComponent(i)).fact).name);
                }
            }
        }

        //Object path = tree.getSelectionPath().getLastPathComponent();
        if ( ! ((path instanceof MondrianGuiDef.Cube) || (path instanceof MondrianGuiDef.Schema) )) {
            JOptionPane.showMessageDialog(this, "Cube or Schema not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }


        MondrianGuiDef.Cube cube = null;
        MondrianGuiDef.Schema schema = null;

        if (path instanceof MondrianGuiDef.Cube) {
            cube = (MondrianGuiDef.Cube) path;
        } else {
            schema = (MondrianGuiDef.Schema) path;
        }

        MondrianGuiDef.NamedSet namedset = new MondrianGuiDef.NamedSet();
        namedset.name = ""; // get unique name// New Named Set " + ((schema==null)?cube.namedSets.length:schema.namedSets.length);
        namedset.formula="";
        namedset.formulaElement = new MondrianGuiDef.Formula();

        //add cube to schema
        if (cube != null) {
            namedset.name = getNewName("New Named Set ", cube.namedSets);

            NodeDef[] temp = cube.namedSets;
            cube.namedSets = new MondrianGuiDef.NamedSet[temp.length + 1];
            for (int i = 0; i < temp.length; i++) {
                cube.namedSets[i] = (MondrianGuiDef.NamedSet) temp[i];}

            cube.namedSets[cube.namedSets.length - 1] = namedset;
        } else {
            namedset.name = getNewName("New Named Set ", schema.namedSets);
            NodeDef[] temp = schema.namedSets;
            schema.namedSets = new MondrianGuiDef.NamedSet[temp.length + 1];
            for (int i = 0; i < temp.length; i++) {
                schema.namedSets[i] = (MondrianGuiDef.NamedSet) temp[i];}

            schema.namedSets[schema.namedSets.length - 1] = namedset;
        }

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(namedset));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addDimensionUsage(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Cube) {
                    //System.out.println("==== path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
                    path = tpath.getPathComponent(parentIndex);
                    //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.Cube)) {
            JOptionPane.showMessageDialog(this, "Cube not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.Cube cube = (MondrianGuiDef.Cube) path;

        MondrianGuiDef.DimensionUsage dimension = new MondrianGuiDef.DimensionUsage();
        dimension.name = ""; //get unique name //"New Dimension Usage" + cube.dimensions.length;

        //add cube to schema
        dimension.name = getNewName("New Dimension Usage ", cube.dimensions);
        NodeDef[] temp = cube.dimensions;
        cube.dimensions = new MondrianGuiDef.CubeDimension[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            cube.dimensions[i] = (MondrianGuiDef.CubeDimension) temp[i];}

        cube.dimensions[cube.dimensions.length - 1] = dimension;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(dimension));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addSchemaGrant(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Role) {
                    //System.out.println("==== path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
                    path = tpath.getPathComponent(parentIndex);
                    //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.Role)) {
            JOptionPane.showMessageDialog(this, "Role not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.Role role = (MondrianGuiDef.Role) path;

        MondrianGuiDef.SchemaGrant schemaGrant = new MondrianGuiDef.SchemaGrant();
        schemaGrant.access = ""; //get unique name //"New Dimension Usage" + cube.dimensions.length;
        schemaGrant.cubeGrants = new MondrianGuiDef.CubeGrant[0];

        //add cube to schema
        NodeDef[] temp = role.schemaGrants;
        role.schemaGrants = new MondrianGuiDef.SchemaGrant[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            role.schemaGrants[i] = (MondrianGuiDef.SchemaGrant) temp[i];}

        role.schemaGrants[role.schemaGrants.length - 1] = schemaGrant;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(schemaGrant));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addCubeGrant(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.SchemaGrant) {
                    //System.out.println("==== path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
                    path = tpath.getPathComponent(parentIndex);
                    //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.SchemaGrant)) {
            JOptionPane.showMessageDialog(this, "Schema Grant not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.SchemaGrant schemaGrant = (MondrianGuiDef.SchemaGrant) path;

        MondrianGuiDef.CubeGrant cubeGrant = new MondrianGuiDef.CubeGrant();
        cubeGrant.access = ""; //get unique name //"New Dimension Usage" + cube.dimensions.length;
        cubeGrant.dimensionGrants = new MondrianGuiDef.DimensionGrant[0];
        cubeGrant.hierarchyGrants= new MondrianGuiDef.HierarchyGrant[0];

        //add cube to schema
        NodeDef[] temp = schemaGrant.cubeGrants;
        schemaGrant.cubeGrants = new MondrianGuiDef.CubeGrant[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            schemaGrant.cubeGrants[i] = (MondrianGuiDef.CubeGrant) temp[i];}

        schemaGrant.cubeGrants[schemaGrant.cubeGrants.length - 1] = cubeGrant;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(cubeGrant));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addDimensionGrant(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.CubeGrant) {
                    //System.out.println("==== path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
                    path = tpath.getPathComponent(parentIndex);
                    //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.CubeGrant)) {
            JOptionPane.showMessageDialog(this, "Cube Grant not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.CubeGrant cubeGrant = (MondrianGuiDef.CubeGrant) path;

        MondrianGuiDef.DimensionGrant dimeGrant = new MondrianGuiDef.DimensionGrant();
        dimeGrant.access = ""; //get unique name //"New Dimension Usage" + cube.dimensions.length;

        //add cube to schema
        NodeDef[] temp = cubeGrant.dimensionGrants;
        cubeGrant.dimensionGrants = new MondrianGuiDef.DimensionGrant[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            cubeGrant.dimensionGrants[i] = (MondrianGuiDef.DimensionGrant) temp[i];}

        cubeGrant.dimensionGrants[cubeGrant.dimensionGrants.length - 1] = dimeGrant;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(dimeGrant));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addHierarchyGrant(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.CubeGrant) {
                    //System.out.println("==== path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
                    path = tpath.getPathComponent(parentIndex);
                    //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.CubeGrant)) {
            JOptionPane.showMessageDialog(this, "Cube Grant not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.CubeGrant cubeGrant = (MondrianGuiDef.CubeGrant) path;

        MondrianGuiDef.HierarchyGrant hieGrant = new MondrianGuiDef.HierarchyGrant();
        hieGrant.access = ""; //get unique name //"New Dimension Usage" + cube.dimensions.length;
        hieGrant.memberGrants = new MondrianGuiDef.MemberGrant[0];

        //add cube to schema
        NodeDef[] temp = cubeGrant.hierarchyGrants;
        cubeGrant.hierarchyGrants = new MondrianGuiDef.HierarchyGrant[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            cubeGrant.hierarchyGrants[i] = (MondrianGuiDef.HierarchyGrant) temp[i];}

        cubeGrant.hierarchyGrants[cubeGrant.hierarchyGrants.length - 1] = hieGrant;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(hieGrant));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addMemberGrant(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.HierarchyGrant) {
                    //System.out.println("==== path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
                    path = tpath.getPathComponent(parentIndex);
                    //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.HierarchyGrant)) {
            JOptionPane.showMessageDialog(this, "Hierarchy Grant not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.HierarchyGrant hieGrant = (MondrianGuiDef.HierarchyGrant) path;

        MondrianGuiDef.MemberGrant memberGrant = new MondrianGuiDef.MemberGrant();
        memberGrant.access = ""; //get unique name //"New Dimension Usage" + cube.dimensions.length;

        //add cube to schema
        NodeDef[] temp = hieGrant.memberGrants;
        hieGrant.memberGrants = new MondrianGuiDef.MemberGrant[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            hieGrant.memberGrants[i] = (MondrianGuiDef.MemberGrant) temp[i];}

        hieGrant.memberGrants[hieGrant.memberGrants.length - 1] = memberGrant;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(memberGrant));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }


    /**
     * @param evt
     */
    protected void addLevel(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Hierarchy) {
                    //System.out.println("==== path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
                    path = tpath.getPathComponent(parentIndex);
                    //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.Hierarchy)) {
            JOptionPane.showMessageDialog(this, "Hierarchy not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.Hierarchy hierarchy = (MondrianGuiDef.Hierarchy) path;

        MondrianGuiDef.Level level = new MondrianGuiDef.Level();
        level.uniqueMembers = false;
        level.name = "";
        level.properties = new MondrianGuiDef.Property[0];
/*
        level.nameExp = new MondrianGuiDef.NameExpression();
        level.nameExp.expressions = new MondrianGuiDef.SQL[1];
        level.nameExp.expressions[0] = new MondrianGuiDef.SQL();
        level.ordinalExp = new MondrianGuiDef.OrdinalExpression();
        level.ordinalExp.expressions = new MondrianGuiDef.SQL[1];
        level.ordinalExp.expressions[0] = new MondrianGuiDef.SQL();
        level.closure = new MondrianGuiDef.Closure();
        level.closure.table = new MondrianGuiDef.Table("","","");
*/
        //dimension.hierarchies[0].memberReaderParameters[0] = new MondrianGuiDef.Parameter();

        //add cube to schema
        level.name = getNewName("New Level ", hierarchy.levels);
        NodeDef[] temp = hierarchy.levels;
        hierarchy.levels = new MondrianGuiDef.Level[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            hierarchy.levels[i] = (MondrianGuiDef.Level) temp[i];}

        hierarchy.levels[hierarchy.levels.length - 1] = level;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(level));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addSQL(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.ExpressionView) {  // parent could also be MondrianGuiDef.Expression
                    //System.out.println("==== path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
                    path = tpath.getPathComponent(parentIndex);
                    //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.ExpressionView)) {
            JOptionPane.showMessageDialog(this, "Expression not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.ExpressionView expview = (MondrianGuiDef.ExpressionView) path;

        MondrianGuiDef.SQL sql = new MondrianGuiDef.SQL();
        sql.dialect="generic";
        //add sql to ExpressionView
        NodeDef[] temp = expview.expressions;
        expview.expressions = new MondrianGuiDef.SQL[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            expview.expressions[i] = (MondrianGuiDef.SQL) temp[i];}

        expview.expressions[expview.expressions.length - 1] = sql;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(sql));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }


    protected void addKeyExp(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Level) {
                    //System.out.println("==== path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
                    path = tpath.getPathComponent(parentIndex);
                    //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.Level)) {
            JOptionPane.showMessageDialog(this, "Level not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.Level level = (MondrianGuiDef.Level) path;

        MondrianGuiDef.KeyExpression keyExp = new MondrianGuiDef.KeyExpression();
        keyExp.expressions = new MondrianGuiDef.SQL[1];    // min 1
        keyExp.expressions[0] = new MondrianGuiDef.SQL();
        keyExp.expressions[0].dialect = "generic";
        keyExp.expressions[0].cdata = "";
        level.keyExp = keyExp;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(keyExp));

        refreshTree(tree.getSelectionPath());
    }

    protected void addNameExp(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Level) {
                    path = tpath.getPathComponent(parentIndex);
                    break;
                }
            }

        }
        if (!(path instanceof MondrianGuiDef.Level)) {
            JOptionPane.showMessageDialog(this, "Level not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.Level level = (MondrianGuiDef.Level) path;

        MondrianGuiDef.NameExpression nameExp = new MondrianGuiDef.NameExpression();
        nameExp.expressions = new MondrianGuiDef.SQL[1];    // min 1
        nameExp.expressions[0] = new MondrianGuiDef.SQL();
        nameExp.expressions[0].dialect = "generic";
        nameExp.expressions[0].cdata = "";
        level.nameExp = nameExp;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(nameExp));

        refreshTree(tree.getSelectionPath());
    }

    protected void addOrdinalExp(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Level) {
                    path = tpath.getPathComponent(parentIndex);
                    break;
                }
            }

        }
        if (!(path instanceof MondrianGuiDef.Level)) {
            JOptionPane.showMessageDialog(this, "Level not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.Level level = (MondrianGuiDef.Level) path;

        MondrianGuiDef.OrdinalExpression ordinalExp = new MondrianGuiDef.OrdinalExpression();
        ordinalExp.expressions = new MondrianGuiDef.SQL[1];    // min 1
        ordinalExp.expressions[0] = new MondrianGuiDef.SQL();
        ordinalExp.expressions[0].dialect = "generic";
        ordinalExp.expressions[0].cdata = "";
        level.ordinalExp = ordinalExp;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(ordinalExp));

        refreshTree(tree.getSelectionPath());
    }

    protected void addParentExp(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Level) {
                    path = tpath.getPathComponent(parentIndex);
                    break;
                }
            }

        }
        if (!(path instanceof MondrianGuiDef.Level)) {
            JOptionPane.showMessageDialog(this, "Level not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.Level level = (MondrianGuiDef.Level) path;

        MondrianGuiDef.ParentExpression parentExp = new MondrianGuiDef.ParentExpression();
        parentExp.expressions = new MondrianGuiDef.SQL[1];    // min 1
        parentExp.expressions[0] = new MondrianGuiDef.SQL();
        parentExp.expressions[0].dialect = "generic";
        parentExp.expressions[0].cdata = "";
        level.parentExp = parentExp;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(parentExp));

        refreshTree(tree.getSelectionPath());
    }

    protected void addMeasureExp(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Measure) {
                    path = tpath.getPathComponent(parentIndex);
                    break;
                }
            }

        }
        if (!(path instanceof MondrianGuiDef.Measure)) {
            JOptionPane.showMessageDialog(this, "Measure not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.Measure measure = (MondrianGuiDef.Measure) path;

        MondrianGuiDef.MeasureExpression measureExp = new MondrianGuiDef.MeasureExpression();
        measureExp.expressions = new MondrianGuiDef.SQL[1];    // min 1
        measureExp.expressions[0] = new MondrianGuiDef.SQL();
        measureExp.expressions[0].dialect = "generic";
        measureExp.expressions[0].cdata = "";
        measure.measureExp = measureExp;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(measureExp));

        refreshTree(tree.getSelectionPath());
    }

    protected void addRelation(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Hierarchy) {
                    //System.out.println("==== path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
                    path = tpath.getPathComponent(parentIndex);
                    //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.Hierarchy)) {
            JOptionPane.showMessageDialog(this, "Hierarchy not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.Hierarchy hierarchy = (MondrianGuiDef.Hierarchy) path;

        MondrianGuiDef.Relation relation = new MondrianGuiDef.Table("", "Table", "");

        //add relation to hierarchy
        hierarchy.relation = relation;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(relation));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    protected void addHierarchy(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Dimension) {
                    //System.out.println("==== path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
                    path = tpath.getPathComponent(parentIndex);
                    //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.Dimension)) {
            JOptionPane.showMessageDialog(this, "Dimension not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.Dimension dimension = (MondrianGuiDef.Dimension) path;

        MondrianGuiDef.Hierarchy hierarchy = new MondrianGuiDef.Hierarchy();

        hierarchy.name = "";// get unique name //"New Hierarchy " + dimension.hierarchies.length;
        hierarchy.hasAll = Boolean.TRUE; //new Boolean(false);
        hierarchy.levels = new MondrianGuiDef.Level[0];
        hierarchy.memberReaderParameters = new MondrianGuiDef.MemberReaderParameter[0];
        hierarchy.relation = new MondrianGuiDef.Table("", "Table", "");

        hierarchy.name = getNewName("New Hierarchy ", dimension.hierarchies);
        NodeDef[] temp = dimension.hierarchies;
        dimension.hierarchies = new MondrianGuiDef.Hierarchy[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            dimension.hierarchies[i] = (MondrianGuiDef.Hierarchy) temp[i];}

        dimension.hierarchies[dimension.hierarchies.length - 1] = hierarchy;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(hierarchy));
        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }


    /**
     * @param evt
     */
    protected void addProperty(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Level) {
                    //System.out.println("==== path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
                    path = tpath.getPathComponent(parentIndex);
                    //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.Level)) {
            JOptionPane.showMessageDialog(this, "Level not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }


        MondrianGuiDef.Level level = (MondrianGuiDef.Level) path;

        MondrianGuiDef.Property property = new MondrianGuiDef.Property();
        property.name = ""; // get unique name // "New Property " + level.properties.length;

        //add cube to schema
        if (level.properties == null) {
            level.properties = new MondrianGuiDef.Property[0];
        }
        property.name = getNewName("New Property ", level.properties);
        NodeDef[] temp = level.properties;
        level.properties = new MondrianGuiDef.Property[temp.length + 1];
        for (int i = 0; i < temp.length; i++) {
            level.properties[i] = (MondrianGuiDef.Property) temp[i];}

        level.properties[level.properties.length - 1] = property;

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(property));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    /**
     * @param evt
     */
    protected void addClosure(ActionEvent evt) {
        TreePath tpath = tree.getSelectionPath();
        int parentIndex = -1;
        Object path = null;
        if (tpath != null) {
            for(parentIndex=tpath.getPathCount()-1; parentIndex>=0;parentIndex--) {
                if (tpath.getPathComponent(parentIndex) instanceof MondrianGuiDef.Level) {
                    //System.out.println("==== path element "+i+" ="+tpath.getPathComponent(i).getClass().toString());
                    path = tpath.getPathComponent(parentIndex);
                    //System.out.println("Cube name ="+((MondrianGuiDef.Cube) path).name);
                    break;
                }
            }

        }
        //Object path = tree.getSelectionPath().getLastPathComponent();
        if (!(path instanceof MondrianGuiDef.Level)) {
            JOptionPane.showMessageDialog(this, "Level not selected.", alert, JOptionPane.WARNING_MESSAGE);
            return;
        }

        MondrianGuiDef.Level level = (MondrianGuiDef.Level) path;
        MondrianGuiDef.Closure closure = new MondrianGuiDef.Closure();
        closure.parentColumn = "";
        closure.childColumn = "";
        closure.table = new MondrianGuiDef.Table("", "Table", "");
        if (level.closure == null) {
            level.closure = closure;
        }

        Object [] parentPathObjs = new Object[parentIndex+1];
        for(int i=0; i <= parentIndex; i++) {
            parentPathObjs[i] = tpath.getPathComponent(i) ;
        }
        TreePath parentPath = new TreePath(parentPathObjs);
        tree.setSelectionPath(parentPath.pathByAddingChild(closure));

        refreshTree(tree.getSelectionPath());
        setTableCellFocus(0);
    }

    public MondrianGuiDef.Schema getSchema() {
        return this.schema;
    }

    /**
     * returns the schema file
     * @return File
     */
    public File getSchemaFile() {
        return this.schemaFile;
    }

    /**
     * sets the schema file
     * @param f
     */
    public void setSchemaFile(File f) {
        this.schemaFile = f;
    }

    /**
     * Called whenever the value of the selection changes.
     * @param e the event that characterizes the change.
     *
     */
    public Object lastSelected;
    public void valueChanged(TreeSelectionEvent e) {
        if (propertyTable.isEditing() && (lastSelected != e.getPath().getLastPathComponent())) {
            SchemaPropertyCellEditor sce = (SchemaPropertyCellEditor) propertyTable.getCellEditor();
            if (sce != null) {
                TreeSelectionEvent e2 = e;
                sce.stopCellEditing();
                e = e2;
            }

        }
        lastSelected = e.getPath().getLastPathComponent();

        String selectedFactTable = null;
        String selectedFactTableSchema = null;

        for(int i=e.getPath().getPathCount()-1; i>=0;i--) { //i=0; i< e.getPath().getPathCount();i++
            //System.out.println("==== path element "+i+" ="+e.getPath().getPathComponent(i).getClass().toString());
            Object comp = e.getPath().getPathComponent(i);
            if ( comp instanceof MondrianGuiDef.Cube &&  ((MondrianGuiDef.Cube) comp).fact != null ) {
                selectedFactTable = ((MondrianGuiDef.Table) ((MondrianGuiDef.Cube) comp).fact).name;
                selectedFactTableSchema = ((MondrianGuiDef.Table) ((MondrianGuiDef.Cube) comp).fact).schema;
                //System.out.println("Cube fact table name ="+((MondrianGuiDef.Table) ((MondrianGuiDef.Cube) e.getPath().getPathComponent(i)).fact).name);
            }
        }
        TreePath tpath = e.getPath();
        Object o = tpath.getLastPathComponent();
        Object po = null;
        // look for parent information
        TreePath parentTpath = tpath.getParentPath();
        String parentName = "";
        String elementName = "";
        if (parentTpath != null) {
            po = parentTpath.getLastPathComponent();
            Class parentClassName = po.getClass();
            try {
                Field nameField = po.getClass().getField("name");
                elementName = (String) nameField.get(po);
                if (elementName == null) {
                    elementName = "";
                } else {
                    elementName = "'" + elementName + "'";
                }
            } catch (Exception ex) {
                elementName = "";
            }
            int pos = parentClassName.toString().lastIndexOf("$");
            if (pos > 0) {
                parentName = parentClassName.toString().substring(pos + 1);
            }
        }

        // Begin : For xml edit mode display
        StringWriter sxml = new StringWriter();
        org.eigenbase.xom.XMLOutput pxml = new org.eigenbase.xom.XMLOutput(sxml);
        pxml.setIndentString("        ");
        // End : For xml edit mode display

        String[] pNames = DEF_DEFAULT;

        validStatusLabel.setText( renderer.invalid(tree, e.getPath(), o) );
        validStatusLabel2.setText( validStatusLabel.getText() );

        if (o instanceof MondrianGuiDef.Column) {
            pNames = DEF_COLUMN;
            targetLabel.setText(LBL_COLUMN);
        } else if (o instanceof MondrianGuiDef.Cube) {
            pNames = DEF_CUBE;
            targetLabel.setText(LBL_CUBE);
            ((MondrianGuiDef.Cube) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.Dimension) {
            pNames = DEF_DIMENSION;
            if (po instanceof MondrianGuiDef.Schema) {
                targetLabel.setText("Shared " + LBL_DIMENSION);
            } else {
                targetLabel.setText(LBL_DIMENSION + " for " + elementName + " " +
                    parentName);
            }
            ((MondrianGuiDef.Dimension) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.DimensionUsage) {
            pNames = DEF_DIMENSION_USAGE;
            targetLabel.setText(LBL_DIMENSION_USAGE + " for " + elementName + " " +
                    parentName);
            ((MondrianGuiDef.DimensionUsage) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.KeyExpression) {
            pNames = DEF_DEFAULT;
            targetLabel.setText(LBL_KEY_EXPRESSION);
            ((MondrianGuiDef.ExpressionView) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.NameExpression) {
            pNames = DEF_DEFAULT;
            targetLabel.setText(LBL_NAME_EXPRESSION);
            ((MondrianGuiDef.ExpressionView) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.OrdinalExpression) {
            pNames = DEF_DEFAULT;
            targetLabel.setText(LBL_ORDINAL_EXPRESSION);
            ((MondrianGuiDef.ExpressionView) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.ParentExpression) {
            pNames = DEF_DEFAULT;
            targetLabel.setText(LBL_PARENT_EXPRESSION);
            ((MondrianGuiDef.ExpressionView) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.ExpressionView) {
            pNames = DEF_EXPRESSION_VIEW;
            targetLabel.setText(LBL_EXPRESSION_VIEW);
            ((MondrianGuiDef.ExpressionView) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.MeasureExpression) {
            pNames = DEF_DEFAULT;
            targetLabel.setText(LBL_MEASURE_EXPRESSION);
            ((MondrianGuiDef.ExpressionView) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.Hierarchy) {
            pNames = DEF_HIERARCHY;
            targetLabel.setText(LBL_HIERARCHY + " for " + elementName + " " +
                    parentName);
            ((MondrianGuiDef.Hierarchy) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.Join) {
            pNames = DEF_JOIN;
            if (parentName.equalsIgnoreCase("Join")) {
                Object parentJoin = parentTpath.getLastPathComponent();
                int indexOfChild = tree.getModel().getIndexOfChild(parentJoin, o);
                switch (indexOfChild) {
                    case 0:
                        targetLabel.setText("Left : " + LBL_JOIN);
                        break;
                    case 1:
                        targetLabel.setText("Right : " + LBL_JOIN);
                }
            } else {
                targetLabel.setText(LBL_JOIN + " for " + elementName + " " +
                    parentName);
            }
            ((MondrianGuiDef.Join) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.Level) {
            pNames = DEF_LEVEL;
            targetLabel.setText(LBL_LEVEL + " for " + elementName + " " +
                    parentName);
            ((MondrianGuiDef.Level) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.Measure) {
            pNames = DEF_MEASURE;
            targetLabel.setText(LBL_MEASURE + " for " + elementName + " " +
                    parentName);
            ((MondrianGuiDef.Measure) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.CalculatedMember) {
            pNames = DEF_CALCULATED_MEMBER;
            targetLabel.setText(LBL_CALCULATED_MEMBER + " for " + elementName + " " +
                    parentName);
            ((MondrianGuiDef.CalculatedMember) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.CalculatedMemberProperty) {
            pNames = DEF_CALCULATED_MEMBER_PROPERTY;
            targetLabel.setText(LBL_CALCULATED_MEMBER_PROPERTY);
        } else if (o instanceof MondrianGuiDef.NamedSet) {
            pNames = DEF_NAMED_SET;
            targetLabel.setText(LBL_NAMED_SET + " for " + elementName + " " +
                    parentName);
            ((MondrianGuiDef.NamedSet) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.UserDefinedFunction) {
            pNames = DEF_USER_DEFINED_FUNCTION;
            targetLabel.setText(LBL_USER_DEFINED_FUNCTION + " for " + elementName +
                    " " + parentName);
            ((MondrianGuiDef.UserDefinedFunction) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.MemberReaderParameter) {
            pNames = DEF_PARAMETER;
            targetLabel.setText(LBL_PARAMETER);
        } else if (o instanceof MondrianGuiDef.Property) {
            pNames = DEF_PROPERTY;
            targetLabel.setText(LBL_PROPERTY);
            ((MondrianGuiDef.Property) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.Closure) {
            pNames = DEF_CLOSURE;
            targetLabel.setText(LBL_CLOSURE);
            ((MondrianGuiDef.Closure) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.Schema) {
            pNames = DEF_SCHEMA;
            targetLabel.setText(LBL_SCHEMA);
            ((MondrianGuiDef.Schema) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.SQL) {
            pNames = DEF_SQL;
            targetLabel.setText(LBL_SQL);
            ((MondrianGuiDef.SQL) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.Table) {
            pNames = DEF_TABLE;
            targetLabel.setText(LBL_TABLE + " for " + elementName + " " +
                    parentName);
            ((MondrianGuiDef.Table) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.AggName) {
            pNames = DEF_AGG_NAME;
            targetLabel.setText(LBL_AGG_NAME);
            ((MondrianGuiDef.AggName) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.AggIgnoreColumn) {
            pNames = DEF_AGG_IGNORE_COLUMN;
            targetLabel.setText(LBL_AGG_IGNORE_COLUMN);
            ((MondrianGuiDef.AggIgnoreColumn) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.AggForeignKey) {
            pNames = DEF_AGG_FOREIGN_KEY;
            targetLabel.setText(LBL_AGG_FOREIGN_KEY);
            ((MondrianGuiDef.AggForeignKey) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.AggMeasure) {
            pNames = DEF_AGG_MEASURE;
            targetLabel.setText(LBL_AGG_MEASURE);
            ((MondrianGuiDef.AggMeasure) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.AggLevel) {
            pNames = DEF_AGG_LEVEL;
            targetLabel.setText(LBL_AGG_LEVEL);
            ((MondrianGuiDef.AggLevel) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.AggExclude) {
            pNames = DEF_AGG_EXCLUDE;
            targetLabel.setText(LBL_AGG_EXCLUDE);
            ((MondrianGuiDef.AggExclude) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.AggPattern) {
            pNames = DEF_AGG_PATTERN;
            targetLabel.setText(LBL_AGG_PATTERN);
            ((MondrianGuiDef.AggPattern) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.AggFactCount) {
            pNames = DEF_AGG_FACT_COUNT;
            targetLabel.setText(LBL_AGG_FACT_COUNT);
            ((MondrianGuiDef.AggFactCount) o).displayXML(pxml, 0);

        } else if (o instanceof MondrianGuiDef.View) {
            pNames = DEF_VIEW;
            targetLabel.setText(LBL_VIEW);

        } else if (o instanceof MondrianGuiDef.Role) {
            pNames = DEF_ROLE;
            targetLabel.setText(LBL_ROLE + " for " + elementName + " " +
                    parentName);
            ((MondrianGuiDef.Role) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.SchemaGrant) {
            pNames = DEF_SCHEMA_GRANT;
            targetLabel.setText(LBL_SCHEMA_GRANT);
            ((MondrianGuiDef.SchemaGrant) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.CubeGrant) {
            pNames = DEF_CUBE_GRANT;
            targetLabel.setText(LBL_CUBE_GRANT);
            ((MondrianGuiDef.CubeGrant) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.DimensionGrant) {
            pNames = DEF_DIMENSION_GRANT;
            targetLabel.setText(LBL_DIMENSION_GRANT);
            ((MondrianGuiDef.DimensionGrant) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.HierarchyGrant) {
            pNames = DEF_HIERARCHY_GRANT;
            targetLabel.setText(LBL_HIERARCHY_GRANT);
            ((MondrianGuiDef.HierarchyGrant) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.MemberGrant) {
            pNames = DEF_MEMBER_GRANT;
            targetLabel.setText(LBL_MEMBER_GRANT);
            ((MondrianGuiDef.MemberGrant) o).displayXML(pxml, 0);

        } else if (o instanceof MondrianGuiDef.VirtualCube) {
            pNames = DEF_VIRTUAL_CUBE;
            targetLabel.setText(LBL_VIRTUAL_CUBE + " for " + elementName + " " +
                    parentName);
            ((MondrianGuiDef.VirtualCube) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.VirtualCubeDimension) {
            pNames = DEF_VIRTUAL_CUBE_DIMENSION;
            targetLabel.setText(LBL_VIRTUAL_CUBE_DIMENSION + " for " + elementName + " " +
                    parentName);
            ((MondrianGuiDef.VirtualCubeDimension) o).displayXML(pxml, 0);
        } else if (o instanceof MondrianGuiDef.VirtualCubeMeasure) {
            pNames = DEF_VIRTUAL_CUBE_MEASURE;
            targetLabel.setText(LBL_VIRTUAL_CUBE_MEASURE);
            ((MondrianGuiDef.VirtualCubeMeasure) o).displayXML(pxml, 0);
        } else {
            targetLabel.setText(LBL_UNKNOWN_TYPE);
        }

        //jEditorPaneXML.setText(sxml.toString());  // removed because it caused the scrollbar for new element selected to reach at the end.
        try {
            jEditorPaneXML.read(new StringReader(sxml.toString()),null);
        } catch(Exception ex) {
            //
        }
        targetLabel2.setText(targetLabel.getText());

        PropertyTableModel ptm = new PropertyTableModel(o, pNames);

        ptm.setFactTable(selectedFactTable);
        ptm.setFactTableSchema(selectedFactTableSchema);

        // generate a list of pre-existing names of siblings in parent component for checking unique names
        Object parent = null;
        for(int i=e.getPath().getPathCount()-1-1; i>=0;i--) {
            parent = e.getPath().getPathComponent(i);   // get parent path
            break;
        }
        if (parent != null) {
            //System.out.println("parent type="+parent.getClass());
            Field[] fs = parent.getClass().getFields();
            ArrayList names = new ArrayList();
            for (int i = 0; i < fs.length; i++) {
                if (fs[i].getType().isArray() && (fs[i].getType().getComponentType().isInstance(o) )) {
                    // selected schema object is an instance of parent's field (an array).
                    //System.out.println("parent Field type="+fs[i].getType().getComponentType());
                    //System.out.println("parent Field name="+fs[i].getName());
                    try {
                        Field fname = fs[i].getType().getComponentType().getField("name"); // name field of array's objects.
                        Object objs = fs[i].get(parent); // get the parent's array of child objects
                        for (int j = 0; j < Array.getLength(objs); j++) {
                            Object child = Array.get(objs, j);
                            Object vname = fname.get(child);
                            names.add(vname);
                        }
                        ptm.setNames(names);
                    } catch(Exception ex) {
                        //name field dosen't exist, skip parent object.
                    }
                    break;
                }
            }
        }

        propertyTable.setModel(ptm);
        propertyTable.getColumnModel().getColumn(0).setMaxWidth(150);
        propertyTable.getColumnModel().getColumn(0).setMinWidth(150);
        //propertyTable.getColumnModel().getColumn(0).setCellRenderer(new SchemaPropertyCellRenderer());

        for (int i = 0; i < propertyTable.getRowCount(); i++) {
            TableCellRenderer renderer = propertyTable.getCellRenderer(i, 1);
            Component comp = renderer.getTableCellRendererComponent(propertyTable, propertyTable.getValueAt(i, 1), false, false, i, 1);
            try {
                int height = comp.getMaximumSize().height;
                propertyTable.setRowHeight(i, height);
            } catch (Exception ea) {
            }
        }
    }

    /**
     * @see javax.swing.event.CellEditorListener#editingCanceled(ChangeEvent)
     */
    public void editingCanceled(ChangeEvent e) {
        updater.update();
    }

    /**
     * @see javax.swing.event.CellEditorListener#editingStopped(ChangeEvent)
     */
    public void editingStopped(ChangeEvent e) {

        setDirty(true);
        if (! dirtyFlag || ((PropertyTableModel) propertyTable.getModel()).target instanceof MondrianGuiDef.Schema) {
            setDirtyFlag(true);   // true means dirty indication shown on title
            setTitle();
        }

        String emsg = ((PropertyTableModel) propertyTable.getModel()).getErrorMsg();
        if (emsg != null) {
            JOptionPane.showMessageDialog(this,emsg , "Error", JOptionPane.ERROR_MESSAGE);
            ((PropertyTableModel) propertyTable.getModel()).setErrorMsg(null);
        }

        updater.update();
    }

    class PopupTrigger extends MouseAdapter {

        public void mouseReleased(MouseEvent e) {
            if (e.isPopupTrigger()) {
                int x = e.getX();
                int y = e.getY();
                TreePath path = tree.getPathForLocation(x, y);
                if (path != null) {
                    jPopupMenu.removeAll();
                    Object pathSelected = path.getLastPathComponent();
                    if (pathSelected instanceof MondrianGuiDef.Schema) {
                        jPopupMenu.add(addCube);
                        jPopupMenu.add(addDimension);
                        jPopupMenu.add(addNamedSet);
                        jPopupMenu.add(addUserDefinedFunction);
                        jPopupMenu.add(jSeparator1);
                        jPopupMenu.add(addVirtualCube);
                        jPopupMenu.add(addRole);
                    } else if (pathSelected instanceof MondrianGuiDef.Cube) {
                        jPopupMenu.add(addDimension);
                        jPopupMenu.add(addDimensionUsage);
                        jPopupMenu.add(addMeasure);
                        jPopupMenu.add(addCalculatedMember);
                        jPopupMenu.add(addNamedSet);
                        jPopupMenu.add(jSeparator1);
                        jPopupMenu.add(delete);
                    } else if (pathSelected instanceof MondrianGuiDef.Dimension) {
                        jPopupMenu.add(addHierarchy);
                        jPopupMenu.add(jSeparator1);
                        jPopupMenu.add(delete);
                    } else if (pathSelected instanceof MondrianGuiDef.Hierarchy) {
                        jPopupMenu.add(addLevel);
                        jPopupMenu.add(addRelation);
                        jPopupMenu.add(jSeparator1);
                        jPopupMenu.add(delete);
                        if (((MondrianGuiDef.Hierarchy) pathSelected).relation == null) {
                            addRelation.setEnabled(true);
                        } else {
                            addRelation.setEnabled(false);
                        }
                    } else if (pathSelected instanceof MondrianGuiDef.Level) {
                        jPopupMenu.add(addProperty);
                        jPopupMenu.add(addKeyExp);
                        if (((MondrianGuiDef.Level) pathSelected).keyExp == null) {
                            addKeyExp.setEnabled(true);
                        } else {
                            addKeyExp.setEnabled(false);
                        }
                        jPopupMenu.add(addNameExp);
                        if (((MondrianGuiDef.Level) pathSelected).nameExp == null) {
                            addNameExp.setEnabled(true);
                        } else {
                            addNameExp.setEnabled(false);
                        }
                        jPopupMenu.add(addOrdinalExp);
                        if (((MondrianGuiDef.Level) pathSelected).ordinalExp == null) {
                            addOrdinalExp.setEnabled(true);
                        } else {
                            addOrdinalExp.setEnabled(false);
                        }
                        jPopupMenu.add(addParentExp);
                        if (((MondrianGuiDef.Level) pathSelected).parentExp == null) {
                            addParentExp.setEnabled(true);
                        } else {
                            addParentExp.setEnabled(false);
                        }
                        jPopupMenu.add(addClosure);
                        if (((MondrianGuiDef.Level) pathSelected).closure == null) {
                            addClosure.setEnabled(true);
                        } else {
                            addClosure.setEnabled(false);
                        }
                        jPopupMenu.add(jSeparator1);
                        jPopupMenu.add(delete);
                    } else if (pathSelected instanceof MondrianGuiDef.KeyExpression ||
                            pathSelected instanceof MondrianGuiDef.NameExpression ||
                            pathSelected instanceof MondrianGuiDef.OrdinalExpression ||
                            pathSelected instanceof MondrianGuiDef.ParentExpression
                            ) {
                        jPopupMenu.add(addSQL);
                        jPopupMenu.add(jSeparator1);
                        jPopupMenu.add(delete);
                    } else if (pathSelected instanceof MondrianGuiDef.Relation) {
                        Object po = path.getParentPath().getLastPathComponent();
                        if (! (po instanceof MondrianGuiDef.Relation) &&
                            ! (po instanceof MondrianGuiDef.Closure)) {
                            if (po instanceof MondrianGuiDef.Cube) {
                                jPopupMenu.add(addAggName);
                                jPopupMenu.add(addAggPattern);
                                jPopupMenu.add(addAggExclude);
                            } else {
                                jPopupMenu.add(delete);
                            }
                        } else {
                            return;
                        }
                    } else if (pathSelected instanceof MondrianGuiDef.Measure) {
                        jPopupMenu.add(addMeasureExp);
                        if (((MondrianGuiDef.Measure) pathSelected).measureExp == null) {
                            addMeasureExp.setEnabled(true);
                        } else {
                            addMeasureExp.setEnabled(false);
                        }
                        jPopupMenu.add(jSeparator1);
                        jPopupMenu.add(delete);
                    } else if (pathSelected instanceof MondrianGuiDef.Closure) {
                        jPopupMenu.add(delete);
                    } else if (pathSelected instanceof MondrianGuiDef.AggName ||
                            pathSelected instanceof MondrianGuiDef.AggPattern) {
                        jPopupMenu.add(addAggFactCount);
                        jPopupMenu.add(addAggIgnoreColumn);
                        jPopupMenu.add(addAggForeignKey);
                        jPopupMenu.add(addAggMeasure);
                        jPopupMenu.add(addAggLevel);
                        if (pathSelected instanceof MondrianGuiDef.AggPattern) {
                            jPopupMenu.add(addAggExclude);
                            if (((MondrianGuiDef.AggPattern) pathSelected).factcount == null) {
                                addAggFactCount.setEnabled(true);
                            } else {
                                addAggFactCount.setEnabled(false);
                            }
                        } else {
                            if (((MondrianGuiDef.AggName) pathSelected).factcount == null) {
                                addAggFactCount.setEnabled(true);
                            } else {
                                addAggFactCount.setEnabled(false);
                            }
                        }
                        jPopupMenu.add(jSeparator1);
                        jPopupMenu.add(delete);
                    } else if (pathSelected instanceof MondrianGuiDef.VirtualCube) {
                        jPopupMenu.add(addVirtualCubeDimension);
                        jPopupMenu.add(addVirtualCubeMeasure);
                        jPopupMenu.add(addCalculatedMember);
                        jPopupMenu.add(jSeparator1);
                        jPopupMenu.add(delete);
                    } else if (pathSelected instanceof MondrianGuiDef.Role) {
                        jPopupMenu.add(addSchemaGrant);
                        jPopupMenu.add(jSeparator1);
                        jPopupMenu.add(delete);
                    } else if (pathSelected instanceof MondrianGuiDef.SchemaGrant) {
                        jPopupMenu.add(addCubeGrant);
                        jPopupMenu.add(jSeparator1);
                        jPopupMenu.add(delete);
                    } else if (pathSelected instanceof MondrianGuiDef.CubeGrant) {
                        jPopupMenu.add(addDimensionGrant);
                        jPopupMenu.add(addHierarchyGrant);
                        jPopupMenu.add(jSeparator1);
                        jPopupMenu.add(delete);
                    } else if (pathSelected instanceof MondrianGuiDef.HierarchyGrant) {
                        jPopupMenu.add(addMemberGrant);
                        jPopupMenu.add(jSeparator1);
                        jPopupMenu.add(delete);
                    } else  {
                        jPopupMenu.add(delete);
                    } /* else {
                        return;
                    }*/
                    jPopupMenu.show(tree, x, y);
                }
            }
        }
    }

    public static final String[] DEF_DEFAULT = {};
    public static final String[] DEF_VIRTUAL_CUBE = { "name", "caption", "enabled"};
    public static final String[] DEF_VIRTUAL_CUBE_MEASURE = { "name", "cubeName", "visible" };
    public static final String[] DEF_VIRTUAL_CUBE_DIMENSION = { "name", "cubeName", "caption", "foreignKey" };
    public static final String[] DEF_VIEW = { "alias" };
    public static final String[] DEF_TABLE = { "schema" , "name", "alias"};
    public static final String[] DEF_AGG_FACT_COUNT = { "column"};
    public static final String[] DEF_AGG_NAME = { "name", "ignorecase"};
    public static final String[] DEF_AGG_PATTERN = { "pattern", "ignorecase"};
    public static final String[] DEF_AGG_EXCLUDE = { "pattern", "name" , "ignorecase"};
    public static final String[] DEF_AGG_IGNORE_COLUMN = { "column"};
    public static final String[] DEF_AGG_FOREIGN_KEY = { "factColumn" , "aggColumn"};
    public static final String[] DEF_AGG_MEASURE = { "column" , "name"};
    public static final String[] DEF_AGG_LEVEL = { "column" , "name"};

    public static final String[] DEF_CLOSURE = { "parentColumn" , "childColumn"};
    public static final String[] DEF_RELATION = { "name" };
    public static final String[] DEF_SQL = { "cdata", "dialect" }; //?
    public static final String[] DEF_SCHEMA = {"name", "measuresCaption", "defaultRole"};
    public static final String[] DEF_PROPERTY = { "name", "column", "type", "formatter", "caption" };
    public static final String[] DEF_PARAMETER = { "name", "value" }; //?
    public static final String[] DEF_MEASURE = { "name", "aggregator", "column", "formatString", "visible", "datatype", "formatter", "caption"};

    public static final String[] DEF_CALCULATED_MEMBER = { "name", "caption", "dimension", "visible", "formula | formulaElement.cdata", "formatString"};
    public static final String[] DEF_FORMULA = { "cdata" };
    public static final String[] DEF_CALCULATED_MEMBER_PROPERTY = { "name", "caption", "expression", "value"};
    public static final String[] DEF_NAMED_SET = { "name", "formula" };
    public static final String[] DEF_USER_DEFINED_FUNCTION = { "name", "className" };

    public static final String[] DEF_LEVEL = { "name", "table", "column", "nameColumn", "parentColumn", "nullParentValue", "ordinalColumn", "type", "uniqueMembers", "levelType","hideMemberIf", "approxRowCount", "caption", "captionColumn", "formatter"};
    public static final String[] DEF_JOIN = { "leftAlias", "leftKey", "rightAlias", "rightKey"};
    public static final String[] DEF_HIERARCHY = { "name", "hasAll", "allMemberName", "allMemberCaption", "allLevelName", "defaultMember", "memberReaderClass", "primaryKeyTable", "primaryKey", "caption" };
    public static final String[] DEF_EXPRESSION_VIEW = {};
    public static final String[] DEF_DIMENSION_USAGE = { "name", "foreignKey", "source", "level", "usagePrefix", "caption" };
    public static final String[] DEF_DIMENSION = { "name", "foreignKey", "type", "usagePrefix", "caption"};
    public static final String[] DEF_CUBE = { "name", "caption", "cache", "enabled" };
    public static final String[] DEF_ROLE = { "name" };
    public static final String[] DEF_SCHEMA_GRANT = { "access" };
    public static final String[] DEF_CUBE_GRANT = { "access", "cube" };
    public static final String[] DEF_DIMENSION_GRANT = { "access", "dimension" };
    public static final String[] DEF_HIERARCHY_GRANT = { "access", "hierarchy", "topLevel", "bottomLevel" };
    public static final String[] DEF_MEMBER_GRANT = { "access", "member" };
    public static final String[] DEF_COLUMN = { "name", "table" };   //?

    private static final String LBL_COLUMN = "Column";
    private static final String LBL_CUBE = "Cube";
    private static final String LBL_ROLE = "Role";
    private static final String LBL_SCHEMA_GRANT = "Schema Grant";
    private static final String LBL_CUBE_GRANT = "Cube Grant";
    private static final String LBL_DIMENSION_GRANT = "Dimension Grant";
    private static final String LBL_HIERARCHY_GRANT = "Hierarchy Grant";
    private static final String LBL_MEMBER_GRANT = "Member Grant";
    private static final String LBL_DIMENSION = "Dimension";
    private static final String LBL_DIMENSION_USAGE = "Dimension Usage";
    private static final String LBL_EXPRESSION_VIEW = "Expression View";
    private static final String LBL_KEY_EXPRESSION = "Key Expression";
    private static final String LBL_NAME_EXPRESSION = "Name Expression";
    private static final String LBL_ORDINAL_EXPRESSION = "Ordinal Expression";
    private static final String LBL_PARENT_EXPRESSION = "Parent Expression";
    private static final String LBL_MEASURE_EXPRESSION = "Measure Expression";
    private static final String LBL_HIERARCHY = "Hierarchy";
    private static final String LBL_JOIN = "Join";
    private static final String LBL_LEVEL = "Level";
    private static final String LBL_MEASURE = "Measure";
    private static final String LBL_CALCULATED_MEMBER = "Calculated Member";
    private static final String LBL_CALCULATED_MEMBER_PROPERTY = "Calculated Member Property";
    private static final String LBL_NAMED_SET = "Named Set";
    private static final String LBL_USER_DEFINED_FUNCTION = "User Defined Function";
    private static final String LBL_PARAMETER = "Parameter";
    private static final String LBL_PROPERTY = "Property";
    private static final String LBL_SCHEMA = "Schema";
    private static final String LBL_SQL = "SQL";
    private static final String LBL_TABLE = "Table";
    private static final String LBL_CLOSURE = "Closure";

    private static final String LBL_AGG_NAME = "Aggregate Name";
    private static final String LBL_AGG_IGNORE_COLUMN = "Aggregate Ignore Column";
    private static final String LBL_AGG_FOREIGN_KEY = "Aggregate Foreign Key";
    private static final String LBL_AGG_MEASURE = "Aggregate Measure";
    private static final String LBL_AGG_LEVEL = "Aggregate Level";
    private static final String LBL_AGG_PATTERN = "Aggregate Pattern";
    private static final String LBL_AGG_EXCLUDE = "Aggregate Exclude";
    private static final String LBL_AGG_FACT_COUNT = "Aggregate Fact Count";

    private static final String LBL_VIEW = "View";
    private static final String LBL_VIRTUAL_CUBE = "Virtual Cube";
    private static final String LBL_VIRTUAL_CUBE_DIMENSION = "Virtual Cube Dimension";
    private static final String LBL_VIRTUAL_CUBE_MEASURE = "Virtual Cube Measure";
    private static final String LBL_UNKNOWN_TYPE = "Unknown Type";

    private static final String alert = "Alert";

    private AbstractAction arrowButtonUpAction;
    private AbstractAction arrowButtonDownAction;

    private AbstractAction addCube;
    private AbstractAction addRole;
    private AbstractAction addSchemaGrant;
    private AbstractAction addCubeGrant;
    private AbstractAction addDimensionGrant;
    private AbstractAction addHierarchyGrant;
    private AbstractAction addMemberGrant;

    private AbstractAction addDimension;
    private AbstractAction addDimensionUsage;
    private AbstractAction addHierarchy;
    private AbstractAction addNamedSet;
    private AbstractAction addUserDefinedFunction;
    private AbstractAction addCalculatedMember;

    private AbstractAction addMeasure;
    private AbstractAction addMeasureExp;
    private AbstractAction addLevel;
    private AbstractAction addSQL;
    private AbstractAction addKeyExp;
    private AbstractAction addNameExp;
    private AbstractAction addOrdinalExp;
    private AbstractAction addParentExp;
    private AbstractAction addRelation;
    private AbstractAction addProperty;
    private AbstractAction addClosure;

    private AbstractAction addAggName;
    private AbstractAction addAggIgnoreColumn;
    private AbstractAction addAggForeignKey;
    private AbstractAction addAggMeasure;
    private AbstractAction addAggLevel;
    private AbstractAction addAggPattern;
    private AbstractAction addAggExclude;
    private AbstractAction addAggFactCount;

    private AbstractAction addVirtualCube;
    private AbstractAction addVirtualCubeDimension;
    private AbstractAction addVirtualCubeMeasure;

    private AbstractAction delete;

    private AbstractAction editMode;

    private JTable propertyTable;
    private JPanel jPanel1;
    private JPanel jPanel2;
    private JPanel jPanel3;
    private JButton addLevelButton;
    private JScrollPane jScrollPane2;
    private JScrollPane jScrollPane1;
    private JButton addPropertyButton;
    private JButton pasteButton;
    private JLabel targetLabel;
    private JLabel validStatusLabel;
    private JLabel targetLabel2;
    private JLabel validStatusLabel2;
    private JTree tree;
    private JSplitPane jSplitPane1;

    private JButton addDimensionButton;
    private JButton addDimensionUsageButton;
    private JButton addHierarchyButton;
    private JButton addNamedSetButton;
    private JButton addUserDefinedFunctionButton;
    private JButton addCalculatedMemberButton;
    private JButton cutButton;
    private JButton addMeasureButton;
    private JButton addCubeButton;
    private JButton addRoleButton;
    private JButton addVirtualCubeButton;
    private JButton addVirtualCubeDimensionButton;
    private JButton addVirtualCubeMeasureButton;

    private JButton deleteButton;
    private JToggleButton editModeButton;
    private JButton copyButton;
    private JToolBar jToolBar1;
    private JPopupMenu jPopupMenu;

    private JSeparator jSeparator1;

    private JPanel footer;
    private JLabel databaseLabel;

    private JPanel jPanelXML;
    private JScrollPane jScrollPaneXML;
    private JEditorPane jEditorPaneXML;

    public boolean isNewFile() {
        return newFile;
    }

    public void setNewFile(boolean newFile) {
        this.newFile = newFile;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public void setTitle() {
        // sets the title of Internal Frame within which this schema explorer is displayed.
        // The title includes schema name and schema file name
        parentIFrame.setTitle("Schema -  "+schema.name+"  "+"("+schemaFile.getName()+")"+(isDirty()?"*":""));
        parentIFrame.setToolTipText(schemaFile.toString());
    }

    public void setDirtyFlag(boolean dirtyFlag) {
        this.dirtyFlag = dirtyFlag;
    }

    public Object getParentObject() {
        TreePath tPath = tree.getSelectionPath();
        if ( (tPath != null) && (tPath.getParentPath() != null) ) {
            return tPath.getParentPath().getLastPathComponent();
        }
        return null;
    }

    public String getJdbcConnectionUrl() {
        return this.jdbcMetaData.jdbcConnectionUrl;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public boolean isEditModeXML() {
        return editModeXML; // used by schema frame focuslistener in workbench/desktoppane
    }

}

// End SchemaExplorer.java
