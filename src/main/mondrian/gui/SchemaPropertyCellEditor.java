/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2008 Julian Hyde and others
// Copyright (C) 2006-2007 CINCOM SYSTEMS, INC.
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
*/
package mondrian.gui;

import java.awt.event.FocusAdapter;
import java.awt.event.ItemListener;
import java.awt.event.MouseAdapter;

import javax.swing.*;
import javax.swing.event.CellEditorListener;
import javax.swing.event.ChangeEvent;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.ArrayList;
import java.util.EventObject;
import java.util.TreeSet;
import java.util.Vector;
import javax.swing.border.*;
import javax.swing.text.JTextComponent;

/**
 *
 * @author  sean
 * @version $Id$
 */
public class SchemaPropertyCellEditor implements javax.swing.table.TableCellEditor {
    Workbench workbench;

    ArrayList listeners;

    JTextField stringEditor;
    JCheckBox booleanEditor;
    JTextField integerEditor;
    JTable tableEditor;
    Component activeEditor;
    JComboBox listEditor;
    JComboBox relationList;  // Join, Table
    JTable relationTable;
    JPanel relationRenderer;

    JDBCMetaData jdbcMetaData;
    ComboBoxModel allOptions, selOptions;
    String listEditorValue ;
    MouseListener ml;
    ItemListener il;
    ActionListener al;

    String noSelect = "-- No Selection --";
    FocusAdapter editorFocus;

    public SchemaPropertyCellEditor(Workbench workbench, JDBCMetaData jdbcMetaData) {
        this(workbench);
        this.jdbcMetaData = jdbcMetaData;
    }

    /** Creates a new instance of SchemaPropertyCellEditor */
    public SchemaPropertyCellEditor(Workbench workbench) {

        this.workbench = workbench;

        noSelect = getResourceConverter().getString("schemaPropertyCellEditor.noSelection",
                "-- No Selection --");

        listeners = new ArrayList();
        stringEditor = new JTextField();
        stringEditor.setFont(Font.decode("Dialog"));
        stringEditor.setBorder(null);

        booleanEditor = new JCheckBox();
        booleanEditor.setBackground(Color.white);

        integerEditor = new JTextField();
        integerEditor.setBorder(null);
        integerEditor.setHorizontalAlignment(JTextField.RIGHT);
        integerEditor.setFont(Font.decode("Courier"));

        tableEditor = new JTable();

        listEditor = new JComboBox();
        listEditor.setEditable(true);
        listEditor.setMaximumSize(stringEditor.getMaximumSize());
        listEditor.setFont(Font.decode("Dialog"));
        listEditor.setBackground(Color.white);
        listEditor.setBorder(new EmptyBorder(0, 0, 0, 0)); //super.noFocusBorder);

        al = new ActionListener() {
            boolean all = true;
            public void actionPerformed(ActionEvent e) {
                if (e.getActionCommand().equals("comboBoxChanged") && listEditor.getSelectedIndex() == 0) {   // 0 index refers to less or more options
                    if (all) {
                        listEditor.setModel(allOptions);
                    } else {
                        listEditor.setModel(selOptions);
                    }
                    listEditor.setSelectedIndex(-1);
                    all = !all;

                }
                if (listEditor.isDisplayable()) {
                    listEditor.setPopupVisible(true);
                }
            }

        };

        JTextComponent editor = (JTextComponent) listEditor.getEditor().getEditorComponent();

        editor.addMouseListener(new MouseAdapter() {
            public void mousePressed(MouseEvent e) {
                if (listEditor.isDisplayable()) {
                    listEditor.setPopupVisible(true);
                }
            }
        });

        editor.addKeyListener(new KeyAdapter() {

            public void keyPressed(KeyEvent e) {
                if (listEditor.isDisplayable()) {
                    listEditor.setPopupVisible(true);
                }
            }
            public void keyReleased(KeyEvent e) {
                //listEditor.setSelectedItem(((JTextComponent) e.getSource()).getText());
                if (e.getKeyCode() == KeyEvent.VK_ESCAPE) {
                    listEditor.setSelectedItem(listEditorValue);
                    listEditor.getEditor().setItem(listEditorValue);
                }
            }
        });

        relationRenderer = new JPanel();

        relationList = new JComboBox(new String[] {getResourceConverter().getString("schemaPropertyCellEditor.join","Join"),
                                                    getResourceConverter().getString("schemaPropertyCellEditor.table","Table")});
        relationList.setMaximumSize(stringEditor.getMaximumSize());
        relationTable = new JTable();
        relationRenderer.add(relationList);
        relationRenderer.add(relationTable);

    }

    public Component getTableCellEditorComponent(final JTable table, Object value, boolean isSelected, final int row, final int column) {

        PropertyTableModel tableModel = (PropertyTableModel) table.getModel();
        Class parentClassz = null;
        if (tableModel.getParentTarget() != null) {
            parentClassz = tableModel.getParentTarget().getClass();}
        Class targetClassz = tableModel.target.getClass();
        String propertyName = tableModel.getRowName(row);
        String selectedFactTable = tableModel.getFactTable();
        String selectedFactTableSchema = tableModel.getFactTableSchema();
        listEditorValue = null;  // reset value of combo-box
        Object parent = this.getParentObject();

        if (targetClassz == MondrianGuiDef.UserDefinedFunction.class && propertyName.equals("className")) {
            Vector udfs = getUdfs();
            ComboBoxModel cAlludfs = new DefaultComboBoxModel(udfs);

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            //listEditor.removeMouseListener(ml);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAlludfs);
            listEditor.setSelectedItem((String)value);
            listEditorValue = (String)value;
            activeEditor = listEditor;
        } else if (targetClassz == MondrianGuiDef.Measure.class && propertyName.equals("formatString")) {
            Vector formatStrs = getFormatStrings();
            ComboBoxModel cAllformatStrs = new DefaultComboBoxModel(formatStrs);

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllformatStrs);
            listEditor.setSelectedItem((String)value);
            listEditorValue = (String)value;
            activeEditor = listEditor;
        } else if (targetClassz == MondrianGuiDef.Measure.class && propertyName.equals("aggregator")) {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(new DefaultComboBoxModel(MondrianGuiDef.Measure._aggregator_values));
            listEditor.setSelectedItem((String)value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.Measure.class && propertyName.equals("datatype")) {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(new DefaultComboBoxModel(MondrianGuiDef.Measure._datatype_values));
            listEditor.setSelectedItem((String)value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.SQL.class && propertyName.equals("dialect")) {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(new DefaultComboBoxModel(MondrianGuiDef.SQL._dialect_values));
            listEditor.setSelectedItem((String)value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.Level.class && propertyName.equals("hideMemberIf")) {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(new DefaultComboBoxModel(MondrianGuiDef.Level._hideMemberIf_values));
            listEditor.setSelectedItem((String)value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.Level.class && propertyName.equals("levelType")) {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(new DefaultComboBoxModel(MondrianGuiDef.Level._levelType_values));
            listEditor.setSelectedItem((String)value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.Level.class && propertyName.equals("type")) {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(new DefaultComboBoxModel(MondrianGuiDef.Level._type_values));
            listEditor.setSelectedItem((String)value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.Dimension.class && propertyName.equals("type")) {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(new DefaultComboBoxModel(MondrianGuiDef.Dimension._type_values));
            listEditor.setSelectedItem((String)value);
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.DimensionUsage.class && propertyName.equals("source")) {
            Vector source = getSource();
            ComboBoxModel cAllsource = new DefaultComboBoxModel(source);

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllsource);
            listEditor.setSelectedItem((String)value);
            listEditorValue = (String)value;
            activeEditor = listEditor;

        } else if ((tableModel.target instanceof MondrianGuiDef.Grant || tableModel.target instanceof MondrianGuiDef.MemberGrant) && propertyName.equals("access")) {

            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            ComboBoxModel cAccess = new DefaultComboBoxModel(MondrianGuiDef.Grant._access_values);

            if (targetClassz == MondrianGuiDef.SchemaGrant.class) {
                cAccess = new DefaultComboBoxModel(new String[] {"all", "none", "all_dimensions"});
            } else if (targetClassz == MondrianGuiDef.CubeGrant.class ||
                    targetClassz == MondrianGuiDef.DimensionGrant.class ||
                    targetClassz == MondrianGuiDef.MemberGrant.class) {
                cAccess = new DefaultComboBoxModel(new String[] {"all", "none"});
            } else if (targetClassz == MondrianGuiDef.HierarchyGrant.class) {
                cAccess = new DefaultComboBoxModel(new String[] {"all", "none", "custom"});
            }
            listEditor.setModel(cAccess);

            listEditor.setSelectedItem((String)value);
            activeEditor = listEditor;

        } else if ((targetClassz ==  MondrianGuiDef.DimensionGrant.class && propertyName.equals("dimension")) ||
                (targetClassz ==  MondrianGuiDef.HierarchyGrant.class && propertyName.equals("hierarchy"))) {
            Vector source = getDimensions();
            ComboBoxModel cAllsource = new DefaultComboBoxModel(source);

            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllsource);
            listEditor.setSelectedItem((String)value);
            listEditorValue = (String)value;
            activeEditor = listEditor;

        } else if ((targetClassz ==  MondrianGuiDef.HierarchyGrant.class && (propertyName.equals("topLevel") || propertyName.equals("bottomLevel")))) {
            Vector source = getLevels(((MondrianGuiDef.HierarchyGrant) tableModel.target).hierarchy);
            ComboBoxModel cAllsource = new DefaultComboBoxModel(source);

            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllsource);
            listEditor.setSelectedItem((String)value);
            listEditorValue = (String)value;
            activeEditor = listEditor;

        } else if (((targetClassz == MondrianGuiDef.VirtualCubeDimension.class || targetClassz == MondrianGuiDef.VirtualCubeMeasure.class) &&
                propertyName.equals("cubeName")) ||
                (targetClassz == MondrianGuiDef.CubeGrant.class && propertyName.equals("cube"))) {
            Vector source = getCubes();
            ComboBoxModel cAllsource = new DefaultComboBoxModel(source);

            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllsource);
            listEditor.setSelectedItem((String)value);
            listEditorValue = (String)value;
            activeEditor = listEditor;
            //===================================================================================
        } else if ((targetClassz == MondrianGuiDef.Dimension.class && propertyName.equals("foreignKey")) ||
                (targetClassz == MondrianGuiDef.DimensionUsage.class && propertyName.equals("foreignKey")) ||
                (targetClassz == MondrianGuiDef.Measure.class && propertyName.equals("column"))) {
            Vector fks      = new Vector(jdbcMetaData.getFactTableFKs(selectedFactTableSchema, selectedFactTable)); //===
            fks.add(0, getResourceConverter().getString("schemaPropertyCellEditor.allColumns","<< All Columns >>"));
            Vector allcols  = new Vector(jdbcMetaData.getAllColumns(selectedFactTableSchema, selectedFactTable));
            ComboBoxModel cFks = new DefaultComboBoxModel(fks);

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            if ((fks.size() > 1) && propertyName.equals("foreignKey")) {
                allcols.add(0, getResourceConverter().getString("schemaPropertyCellEditor.foreignKeys", "<< Foreign keys >>"));
                ComboBoxModel cAllcols = new DefaultComboBoxModel(allcols);
                listEditor.setModel(cFks);
                //listEditor.setToolTipText("Relavant Options shows Foreign keys and All Columns in selected Fact Table");
                selOptions = cFks;
                allOptions = cAllcols;
                //listEditor.addItemListener(il);     // executes twice for each selection
                //listEditor.addMouseListener(ml);    //does not execute when ComboBox isEditable true.
                listEditor.addActionListener(al);
            } else {
                ComboBoxModel cAllcols = new DefaultComboBoxModel(allcols);
                listEditor.setModel(cAllcols);
            }

            listEditor.setSelectedItem((String)value);
            listEditorValue = (String)value;
            activeEditor = listEditor;
        } else if (targetClassz == MondrianGuiDef.Hierarchy.class && propertyName.equals("primaryKey")) {
            MondrianGuiDef.Hierarchy hProps = (MondrianGuiDef.Hierarchy) tableModel.getValue();
            String pkTable = hProps.primaryKeyTable;

            String schemaName = null;
            Vector allcols  = jdbcMetaData.getAllColumns(schemaName, pkTable);
            String pk = jdbcMetaData.getTablePK(schemaName, pkTable);

            ComboBoxModel cAllcols = new DefaultComboBoxModel(allcols);

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllcols);
            if (value == null || ((String) value).equals("")) {
                listEditor.setSelectedItem(pk);
            } else {
                listEditor.setSelectedItem((String)value);
                listEditorValue = (String)value;
            }
            activeEditor = listEditor;
        } else if ((targetClassz == MondrianGuiDef.Level.class && propertyName.equals("column")) ||
                (targetClassz == MondrianGuiDef.Level.class && propertyName.equals("nameColumn")) ||
                (targetClassz == MondrianGuiDef.Level.class && propertyName.equals("parentColumn")) ||
                (targetClassz == MondrianGuiDef.Level.class && propertyName.equals("ordinalColumn")) ||
                (targetClassz == MondrianGuiDef.Level.class && propertyName.equals("captionColumn")) ||
                (targetClassz == MondrianGuiDef.Closure.class && propertyName.equals("parentColumn")) ||
                (targetClassz == MondrianGuiDef.Closure.class && propertyName.equals("childColumn")) ||
                (targetClassz == MondrianGuiDef.Property.class && propertyName.equals("column"))) {
            MondrianGuiDef.Level lProps;
            if (targetClassz == MondrianGuiDef.Level.class) {
                lProps = (MondrianGuiDef.Level) tableModel.getValue();
            } else {
                lProps = (MondrianGuiDef.Level) this.getParentObject();
            }

            String lTable = lProps.table;
            Vector allcols;
            //EC: Sets the corresponding columns on the selection dropdown for the specified table.
            if (targetClassz == MondrianGuiDef.Level.class && parent != null) {
                if (parent instanceof MondrianGuiDef.Hierarchy) {
                    MondrianGuiDef.RelationOrJoin relation = ((MondrianGuiDef.Hierarchy) parent).relation;
                    if (relation instanceof MondrianGuiDef.Table) {
                        lTable = ((MondrianGuiDef.Table) relation).name;
                    } else if (relation instanceof MondrianGuiDef.Join) {
                        lTable = SchemaExplorer.getTableNameForAlias(relation, lTable);
                    }
                }
            }
            if (lTable != null) {
                allcols  = jdbcMetaData.getAllColumns(null, lTable);
            } else {
                allcols  = jdbcMetaData.getAllColumns(selectedFactTableSchema, null);
            }
            ComboBoxModel cAllcols = new DefaultComboBoxModel(allcols);

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllcols);
            listEditor.setSelectedItem((String)value);
            listEditorValue = (String)value;
            activeEditor = listEditor;
            //===================================================================================

        } else if (targetClassz == MondrianGuiDef.Property.class && propertyName.equals("type")) {
            listEditor.setEditable(false);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(new DefaultComboBoxModel(MondrianGuiDef.Property._type_values));
            listEditor.setSelectedItem((String)value);
            activeEditor = listEditor;

        } else if ((targetClassz == MondrianGuiDef.AggFactCount.class && propertyName.equals("column")) ||
                   (targetClassz == MondrianGuiDef.AggIgnoreColumn.class && propertyName.equals("column")) ||
                   (targetClassz == MondrianGuiDef.AggLevel.class && propertyName.equals("column")) ||
                   (targetClassz == MondrianGuiDef.AggMeasure.class && propertyName.equals("column")) ||
                   (targetClassz == MondrianGuiDef.AggForeignKey.class && propertyName.equals("factColumn")) ||
                   (targetClassz == MondrianGuiDef.AggForeignKey.class && propertyName.equals("aggColumn"))) {
            Vector allcols;
            allcols  = jdbcMetaData.getAllColumns(null, null);
            ComboBoxModel cAllcols = new DefaultComboBoxModel(allcols);

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllcols);
            listEditor.setSelectedItem((String)value);
            listEditorValue = (String)value;
            activeEditor = listEditor;

        } else if (targetClassz == MondrianGuiDef.Table.class && propertyName.equals("schema")) {
            Vector allschemas  = jdbcMetaData.getAllSchemas();
            ComboBoxModel cAllschemas = new DefaultComboBoxModel(allschemas);

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);

            listEditor.setModel(cAllschemas);
            listEditor.setSelectedItem((String)value);
            listEditorValue = (String)value;
            activeEditor = listEditor;
        } else if ((targetClassz == MondrianGuiDef.Table.class && propertyName.equals("name")) ||
            (targetClassz == MondrianGuiDef.Hierarchy.class && propertyName.equals("primaryKeyTable")) ||
            (targetClassz == MondrianGuiDef.Level.class && propertyName.equals("table"))) {
            String schema = "";
            if (targetClassz == MondrianGuiDef.Table.class) {
                MondrianGuiDef.Table tProps = (MondrianGuiDef.Table) tableModel.getValue();
                schema = tProps.schema;
            }
            Vector factTables           = new Vector(jdbcMetaData.getFactTables(schema));
            Vector allTablesMinusFact   = new Vector(jdbcMetaData.getAllTables(schema, selectedFactTable));
            Vector allTables            = new Vector(jdbcMetaData.getAllTables(schema));
            Vector dimeTables           = new Vector(jdbcMetaData.getDimensionTables(schema, selectedFactTable));

            ComboBoxModel cFactTables = new DefaultComboBoxModel(factTables);   //suggestive fact tables
            ComboBoxModel cAllTables  = new DefaultComboBoxModel((allTablesMinusFact.size() > 0) ? allTablesMinusFact : allTables);  // all tables of selected schema
            ComboBoxModel cDimeTables = new DefaultComboBoxModel(dimeTables); // suggestive dimension tables based on selected fact table .
            //EC: Sets the corresponding join tables on selection dropdown when using joins.
            if (targetClassz == MondrianGuiDef.Level.class && parent != null) {
                if (parent instanceof MondrianGuiDef.Hierarchy) {
                    MondrianGuiDef.RelationOrJoin relation = ((MondrianGuiDef.Hierarchy) parent).relation;
                    if (relation instanceof MondrianGuiDef.Join) {
                        TreeSet joinTables = new TreeSet();
                        //EC: getTableNamesForJoin calls itself recursively and collects table names in joinTables.
                        SchemaExplorer.getTableNamesForJoin(relation, joinTables);
                        cAllTables  = new DefaultComboBoxModel(new Vector(joinTables));
                    }
                }
            }

            listEditor.setEditable(true);
            listEditor.setToolTipText(null);
            listEditor.removeActionListener(al);
            listEditor.setModel(cAllTables);
            allOptions = cAllTables;
            boolean toggleModel = false;
            if (parentClassz == MondrianGuiDef.Cube.class) {
                cAllTables  = new DefaultComboBoxModel(allTables);
                allOptions = cAllTables;
                if (factTables.size() > 0) {
                    ((DefaultComboBoxModel) cFactTables).insertElementAt(workbench.getResourceConverter().getString("schemaPropertyCellEditor.allTables","<< All Tables >>"),0);
                    ((DefaultComboBoxModel) cAllTables).insertElementAt(workbench.getResourceConverter().getString("schemaPropertyCellEditor.factTables","<< Fact Tables >>"),0);
                    listEditor.setModel(cFactTables);
                    //listEditor.setToolTipText("Double-Click to toggle between Fact tables and All tables");
                    selOptions = cFactTables;
                    toggleModel = true;
                }
            } else {
                if (dimeTables.size() > 0) {
                    ((DefaultComboBoxModel) cDimeTables).insertElementAt(workbench.getResourceConverter().getString("schemaPropertyCellEditor.allTables","<< All Tables >>"),0);
                    ((DefaultComboBoxModel) cAllTables).insertElementAt(workbench.getResourceConverter().getString("schemaPropertyCellEditor.dimensionTables","<< Dimension Tables >>"),0);
                    listEditor.setModel(cDimeTables);
                    //listEditor.setToolTipText("Double-Click to toggle between Dimension tables and All tables");
                    selOptions = cDimeTables;
                    toggleModel = true;
                }
            }

            if (toggleModel) {
                listEditor.addActionListener(al);}
            listEditor.setSelectedItem((String)value);
            listEditorValue = (String)value;
            activeEditor = listEditor;
            //EC: Disables table selection when not using joins.
            if (targetClassz == MondrianGuiDef.Level.class && propertyName.equals(SchemaExplorer.DEF_LEVEL[1]) && parent != null) {
                if (parent instanceof MondrianGuiDef.Hierarchy) {
                    MondrianGuiDef.RelationOrJoin relation = ((MondrianGuiDef.Hierarchy) parent).relation;
                    if (relation instanceof MondrianGuiDef.Table) {
                        activeEditor = stringEditor;
                        stringEditor.setText((String)value);
                    }
                }
            }
        } else if (value instanceof String) {
            activeEditor = stringEditor;
            stringEditor.setText((String)value);
        } else if (value instanceof Boolean) {
            activeEditor = booleanEditor;
            booleanEditor.setSelected((Boolean) value);
        } else if (value instanceof Integer) {
            activeEditor = integerEditor;
            integerEditor.setText((String)value);
        } else if (value == null) {
            value = "";
            activeEditor = stringEditor;
            stringEditor.setText((String)value);
        } else if (value.getClass() == MondrianGuiDef.Join.class) {
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor(workbench);
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm = new PropertyTableModel(workbench, value,SchemaExplorer.DEF_JOIN);
            tableEditor.setModel(ptm);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianGuiDef.NameExpression.class) {
            return null;
        } else if (value.getClass() == MondrianGuiDef.RelationOrJoin.class) {
            // REVIEW: I don't think this code will ever be executed, because
            // class RelationOrJoin is abstract.
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor(workbench);
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm = new PropertyTableModel(workbench, value,SchemaExplorer.DEF_RELATION);
            tableEditor.setModel(ptm);
            activeEditor = tableEditor;
            return null;
        } else if (value.getClass() == MondrianGuiDef.OrdinalExpression.class) {
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor(workbench);
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            //===PropertyTableModel ptm = new PropertyTableModel(value,SchemaExplorer.DEF_SQL);
            PropertyTableModel ptm = new PropertyTableModel(workbench, ((MondrianGuiDef.OrdinalExpression)value).expressions[0],SchemaExplorer.DEF_SQL);
            ptm.setParentTarget(((PropertyTableModel) table.getModel()).target);
            tableEditor.setModel(ptm);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianGuiDef.Formula.class) {
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor(workbench, jdbcMetaData);
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm = new PropertyTableModel(workbench, value,SchemaExplorer.DEF_FORMULA);
            tableEditor.setModel(ptm);
            tableEditor.getColumnModel().getColumn(0).setMaxWidth(100);
            tableEditor.getColumnModel().getColumn(0).setMinWidth(100);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianGuiDef.CalculatedMemberProperty.class) {
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor(workbench, jdbcMetaData);
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm = new PropertyTableModel(workbench, value,SchemaExplorer.DEF_CALCULATED_MEMBER_PROPERTY);
            tableEditor.setModel(ptm);
            tableEditor.getColumnModel().getColumn(0).setMaxWidth(100);
            tableEditor.getColumnModel().getColumn(0).setMinWidth(100);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianGuiDef.Table.class) {
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor(workbench, jdbcMetaData);
            // adding cell editing stopped listeners to nested property of type table
            // so that any change in value of table fields are reflected in tree
            for (int i = listeners.size() - 1; i >= 0; i--) {
                spce.addCellEditorListener(((CellEditorListener)listeners.get(i)));
            }
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm = new PropertyTableModel(workbench, value,SchemaExplorer.DEF_TABLE);
            ptm.setFactTable(selectedFactTable);
            if (targetClassz == MondrianGuiDef.Cube.class) {
                ptm.setParentTarget(((PropertyTableModel) table.getModel()).target);}
            tableEditor.setModel(ptm);
            tableEditor.getColumnModel().getColumn(0).setMaxWidth(100);
            tableEditor.getColumnModel().getColumn(0).setMinWidth(100);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianGuiDef.AggFactCount.class) {
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor(workbench, jdbcMetaData);
            // adding cell editing stopped listeners to nested property of type table
            // so that any change in value of table fields are reflected in tree
            for (int i = listeners.size() - 1; i >= 0; i--) {
                spce.addCellEditorListener(((CellEditorListener)listeners.get(i)));
            }
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm = new PropertyTableModel(workbench, value,SchemaExplorer.DEF_AGG_FACT_COUNT);
            ptm.setFactTable(selectedFactTable);
            /*
            if (targetClassz == MondrianGuiDef.Cube.class) {
                ptm.setParentTarget(((PropertyTableModel) table.getModel()).target);}
             */
            tableEditor.setModel(ptm);
            tableEditor.getColumnModel().getColumn(0).setMaxWidth(100);
            tableEditor.getColumnModel().getColumn(0).setMinWidth(100);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianGuiDef.Closure.class) {
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor(workbench, jdbcMetaData);
            // adding cell editing stopped listeners to nested property of type table
            // so that any change in value of table fields are reflected in tree
            for (int i = listeners.size() - 1; i >= 0; i--) {
                spce.addCellEditorListener(((CellEditorListener)listeners.get(i)));
            }
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm = new PropertyTableModel(workbench, value,SchemaExplorer.DEF_CLOSURE);
            ptm.setFactTable(selectedFactTable);
            if (targetClassz == MondrianGuiDef.Level.class) {
                ptm.setParentTarget(((PropertyTableModel) table.getModel()).target);}
            tableEditor.setModel(ptm);
            tableEditor.getColumnModel().getColumn(0).setMaxWidth(100);
            tableEditor.getColumnModel().getColumn(0).setMinWidth(100);
            spcr.setTableRendererHeight(tableEditor, null);
            activeEditor = tableEditor;
        } else if (value.getClass() == MondrianGuiDef.Property.class) {
            SchemaPropertyCellEditor spce = new SchemaPropertyCellEditor(workbench);
            tableEditor.setDefaultEditor(Object.class, spce);
            SchemaPropertyCellRenderer spcr = new SchemaPropertyCellRenderer(workbench);
            tableEditor.setDefaultRenderer(Object.class, spcr);
            PropertyTableModel ptm = new PropertyTableModel(workbench, value,SchemaExplorer.DEF_PROPERTY);
            tableEditor.setModel(ptm);
            activeEditor = tableEditor;
        } else {
            value = "";
            activeEditor = stringEditor;
            stringEditor.setText((String)value);
        }
        activeEditor.setVisible(true);

        /*  Focus lost on table was supposed to save the last edited value in table model object
                 but this activeeditor saved the last edited value in the new table model object
            activeEditor.addFocusListener(new FocusAdapter() {
                public void focusGained(FocusEvent e) {System.out.println("**Editor GAINED focus "+this.getClass()); }
                public void focusLost(FocusEvent e) {System.out.println("**Editor LOST focus "+this.getClass());

                    Object value = table.getCellEditor(row, column).getCellEditorValue();
                    table.getModel().setValueAt(value, row, column);
                        Object value = table.getCellEditor().getCellEditorValue();
                        table.setValueAt(value, row, column);
                }
            });
         */

        table.changeSelection(row, column, false, false);
        activeEditor.setBackground(new java.awt.Color(224, 249, 255));
        activeEditor.requestFocusInWindow();
        return activeEditor;
    }

    /** Adds a listener to the list that's notified when the editor
     * stops, or cancels editing.
     *
     * @param   l       the CellEditorListener
     *
     */
    public void addCellEditorListener(CellEditorListener l) {
        listeners.add(l);
    }

    /** Tells the editor to cancel editing and not accept any partially
     * edited value.
     *
     */
    public void cancelCellEditing() {
        if (activeEditor != null) {
            activeEditor.setVisible(false);
            fireEditingCancelled();
        }
    }

    /** Returns the value contained in the editor.
     * @return the value contained in the editor
     *
     */
    public Object getCellEditorValue() {
        if (activeEditor == stringEditor) {
            return stringEditor.getText();
        } else if (activeEditor == booleanEditor) {
            return booleanEditor.isSelected();
        } else if (activeEditor == listEditor) {
            if (listEditor.isEditable()) {
                return listEditor.getEditor().getItem();   // returns the edited value from combox box
            } else {
                if (listEditor.getSelectedItem() == noSelect) {
                    return null;  // blank selection
                }
                return listEditor.getSelectedItem(); //// returns the selected value from combox box
            }
        } else if (activeEditor == tableEditor) {
            return ((PropertyTableModel) tableEditor.getModel()).getValue();
        }

        return null;
    }

    /** Asks the editor if it can start editing using <code>anEvent</code>.
     * <code>anEvent</code> is in the invoking component coordinate system.
     * The editor can not assume the Component returned by
     * <code>getCellEditorComponent</code> is installed.  This method
     * is intended for the use of client to avoid the cost of setting up
     * and installing the editor component if editing is not possible.
     * If editing can be started this method returns true.
     *
     * @param   anEvent     the event the editor should use to consider
     *              whether to begin editing or not
     * @return  true if editing can be started
     * @see #shouldSelectCell
     *
     */
    public boolean isCellEditable(EventObject anEvent) {
        return true;
    }

    /** Removes a listener from the list that's notified
     *
     * @param   l       the CellEditorListener
     *
     */
    public void removeCellEditorListener(CellEditorListener l) {
        listeners.remove(l);
    }

    /** Returns true if the editing cell should be selected, false otherwise.
     * Typically, the return value is true, because is most cases the editing
     * cell should be selected.  However, it is useful to return false to
     * keep the selection from changing for some types of edits.
     * eg. A table that contains a column of check boxes, the user might
     * want to be able to change those checkboxes without altering the
     * selection.  (See Netscape Communicator for just such an example)
     * Of course, it is up to the client of the editor to use the return
     * value, but it doesn't need to if it doesn't want to.
     *
     * @param   anEvent     the event the editor should use to start
     *              editing
     * @return  true if the editor would like the editing cell to be selected;
     *    otherwise returns false
     * @see #isCellEditable
     *
     */
    public boolean shouldSelectCell(EventObject anEvent) {
        return true;
    }

    /** Tells the editor to stop editing and accept any partially edited
     * value as the value of the editor.  The editor returns false if
     * editing was not stopped; this is useful for editors that validate
     * and can not accept invalid entries.
     *
     * @return  true if editing was stopped; false otherwise
     *
     */
    public boolean stopCellEditing() {
        if (activeEditor != null) {
            /* save the nested table as well */
            if (activeEditor == tableEditor) {
                if (tableEditor.isEditing()) {
                    /*
                    System.out.println("    tableEditor.isEditing()=="+tableEditor.isEditing());
                    System.out.println("    tableEditor.getEditingRow()=="+tableEditor.getEditingRow());
                    System.out.println("    tableEditor.getEditingColumn()=="+tableEditor.getEditingColumn());
                     */
                    ArrayList nestedTableEditors = new ArrayList();
                    JTable nestedTableEditor = tableEditor;
                    // get the list of nested tables from outer->inner sequence, descending towards innermost nested table
                    // so that we can stop the editing in this order.
                    while (nestedTableEditor != null) {
                        nestedTableEditors.add(nestedTableEditor);
                        SchemaPropertyCellEditor sce = (SchemaPropertyCellEditor) nestedTableEditor.getCellEditor();
                        if (sce != null && sce.activeEditor == sce.tableEditor && sce.tableEditor.isEditing()) {
                            nestedTableEditor = sce.tableEditor; //
                            //tableEditor.editingStopped(null);
                        } else {
                            nestedTableEditor = null;
                        }
                    }
                    for (int i = nestedTableEditors.size() - 1; i >= 0; i--) {
                        ((JTable) nestedTableEditors.get(i)).editingStopped(null);
                    }
                        /*
                        SchemaPropertyCellEditor sce = (SchemaPropertyCellEditor) tableEditor.getCellEditor();
                        if (sce != null) {
                            //tableEditor.editingStopped(null);
                            Object value = sce.getCellEditorValue();
                            tableEditor.setValueAt(value, tableEditor.getEditingRow(), tableEditor.getEditingColumn());
                            //sce.stopCellEditing();
                        }
                         **/

                }

            }
            activeEditor.setVisible(false);
            fireEditingStopped();
        }
        return true;
    }

    protected void fireEditingStopped() {
        ChangeEvent ce = new ChangeEvent(this);
        for (int i = listeners.size() - 1; i >= 0; i--) {
            ((CellEditorListener)listeners.get(i)).editingStopped(ce);
        }
    }

    protected void fireEditingCancelled() {
        ChangeEvent ce = new ChangeEvent(this);
        for (int i = listeners.size() - 1; i >= 0; i--) {
            ((CellEditorListener)listeners.get(i)).editingCanceled(ce);
        }
    }
    private Vector getUdfs() {
        Vector udfs = new Vector();
        MondrianGuiDef.Schema s = this.getSchema();
        if (s != null) {
            MondrianGuiDef.UserDefinedFunction [] u = s.userDefinedFunctions;
            for (int i = 0; i < u.length; i++) {
                if (!(u[i].className == null || udfs.contains(u[i].className))) {
                    udfs.add(u[i].className);
                }
            }
        }

        return udfs;
    }

    private Vector getFormatStrings() {
        Vector fs = new Vector();
        MondrianGuiDef.Schema s = this.getSchema();
        if (s != null) {
            MondrianGuiDef.Cube [] c = s.cubes;
            for (int i = 0; i < c.length; i++) {
                MondrianGuiDef.Measure [] m = c[i].measures;
                for (int j = 0; j < m.length; j++) {
                    if (!(m[j].formatString == null || fs.contains(m[j].formatString))) {
                        fs.add(m[j].formatString);
                    }
                }
            }
        }
        return fs;
    }

    private MondrianGuiDef.Schema getSchema() {
        SchemaExplorer se = workbench.getCurrentSchemaExplorer();
        return (se == null) ? null : se.getSchema();
    }

    private Object getParentObject() {
        SchemaExplorer se = workbench.getCurrentSchemaExplorer();
        if (se != null) {
            Object po = se.getParentObject();
            return po;
        }
        return null;
    }

    private Vector getSource() {    //shared dimensions in schema
        Vector source = new Vector();
        MondrianGuiDef.Schema s = this.getSchema();
        if (s != null) {
            MondrianGuiDef.Dimension [] u = s.dimensions;
            for (int i = 0; i < u.length; i++) {
                source.add(u[i].name);
            }
        }
        return source;
    }

    private Vector getCubes() {
        Vector source = new Vector();
        //===source.add(noSelect);
        MondrianGuiDef.Schema s = this.getSchema();
        if (s != null) {
            MondrianGuiDef.Cube [] u = s.cubes;
            for (int i = 0; i < u.length; i++) {
                source.add(u[i].name);
            }
        }
        return source;
    }

    private void generatePrimaryKeyTables(Object relation, Vector v) {
        if (relation == null) {
            return;
        }
        if (relation instanceof MondrianGuiDef.Table) {
            String sname = ((MondrianGuiDef.Table) relation).schema;
            v.add(((sname == null || sname.equals("")) ? "" : sname + "->") + ((MondrianGuiDef.Table) relation).name);
            return;
        }
        MondrianGuiDef.Join currentJoin = (MondrianGuiDef.Join)relation;
        generatePrimaryKeyTables(currentJoin.left, v);
        generatePrimaryKeyTables(currentJoin.right, v);
        return ;
    }

    private Vector getDimensions() {
        Vector dims = new Vector();
        Object po = getParentObject() ; //cubegrant
        if (po != null) {
            MondrianGuiDef.CubeGrant parent = (MondrianGuiDef.CubeGrant) po;
            if (! (parent.cube == null || parent.cube.equals(""))) {
                MondrianGuiDef.Schema s = getSchema();
                if (s != null) {
                    for (int i = 0; i < s.cubes.length ; i++) {
                        if (s.cubes[i].name.equals(parent.cube)) {
                            for (int j = 0; j < s.cubes[i].dimensions.length; j++) {
                                dims.add(s.cubes[i].dimensions[j].name);
                            }
                            break;
                        }
                    }
                }
            }
        }
        return dims;
    }

    private String cacheCube = "";
    private String cacheHierarchy = "";
    private Vector hlevels = new Vector();

    private Vector getLevels(String hierarchy) {
        if (! (hierarchy == null || hierarchy.equals(""))) {
            if (hierarchy.startsWith("[") && hierarchy.endsWith("]")) {
                hierarchy = hierarchy.substring(1, hierarchy.length() - 1);
            }
            Object po = getParentObject() ; //cubegrant
            if (po != null) {
                MondrianGuiDef.CubeGrant parent = (MondrianGuiDef.CubeGrant) po;
                if (! (parent.cube == null || parent.cube.equals(""))) {
                    if (cacheCube.equals(parent.cube) && cacheHierarchy.equals(hierarchy)) {
                        return hlevels;
                    } else {
                        hlevels = new Vector();
                        cacheCube = parent.cube;
                        cacheHierarchy = hierarchy;
                        MondrianGuiDef.Schema s = getSchema();
                        if (s != null) {
                            // search for cube in schema
                            for (int i = 0; i < s.cubes.length; i++) {
                                if (s.cubes[i].name.equals(parent.cube)) {
                                    // serach for hierarchy in cube
                                    for (int j = 0; j < s.cubes[i].dimensions.length; j++) {
                                        if (s.cubes[i].dimensions[j].name.equals(hierarchy)) {
                                            MondrianGuiDef.Dimension d = null;
                                            if (s.cubes[i].dimensions[j] instanceof MondrianGuiDef.Dimension) {
                                               d =  (MondrianGuiDef.Dimension)s.cubes[i].dimensions[j];
                                            } else {
                                               MondrianGuiDef.DimensionUsage d2 = (MondrianGuiDef.DimensionUsage)s.cubes[i].dimensions[j];
                                               for (int m = 0; m < s.dimensions.length; m++) {
                                                    if (s.dimensions[m].name.equals(d2.source)) {
                                                        d = s.dimensions[m];
                                                        break;
                                                    }
                                               }
                                            }
                                            if (d.hierarchies[0] != null) {
                                                for (int k = 0; k < d.hierarchies[0].levels.length; k++) {
                                                    hlevels.add(d.hierarchies[0].levels[k].name);
                                                }
                                            }
                                            break;
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        return hlevels;
    }

    private I18n getResourceConverter() {
        return workbench.getResourceConverter();
    }

    /* // Not required for time being
    class MapComboBoxModel extends DefaultComboBoxModel {
        Map objectsMap;

        public MapComboBoxModel(Map m) {
            super(m.keySet().toArray());
            objectsMap = m;
        }

        public Object getListEditorValue() {
            return objectsMap.get(super.getSelectedItem());
        }
    }
     */
}


// End SchemaPropertyCellEditor.java
