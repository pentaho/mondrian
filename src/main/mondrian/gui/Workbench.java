/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 1999-2005 Julian Hyde
// Copyright (C) 2005-2013 Pentaho and others
// Copyright (C) 2006-2007 Cincom Systems, Inc.
// Copyright (C) 2006-2007 JasperSoft
// All Rights Reserved.
*/
package mondrian.gui;

import mondrian.olap.DriverManager;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util;
import mondrian.olap.Util.PropertyList;
import mondrian.util.UnionIterator;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import org.eigenbase.xom.XMLOutput;
import org.eigenbase.xom.*;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.ui.database.DatabaseConnectionDialog;
import org.pentaho.ui.database.Messages;
import org.pentaho.ui.database.event.DataHandler;
import org.pentaho.ui.xul.*;
import org.pentaho.ui.xul.containers.XulDialog;
import org.pentaho.ui.xul.swing.SwingXulLoader;

import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.List;
import javax.swing.*;
import javax.swing.event.InternalFrameAdapter;
import javax.swing.event.InternalFrameEvent;
import javax.swing.filechooser.FileSystemView;
import javax.swing.plaf.basic.BasicArrowButton;
import javax.swing.text.DefaultEditorKit;
import javax.swing.tree.TreePath;

/**
 * @author sean
 */
public class Workbench extends javax.swing.JFrame {

    private static final long serialVersionUID = 1L;
    static String WORKBENCH_USER_HOME_DIR;
    static String WORKBENCH_CONFIG_FILE;
    static String DB_META_CONFIG_FILE;

    private static final String LAST_USED1 = "lastUsed1";
    private static final String LAST_USED1_URL = "lastUsedUrl1";
    private static final String LAST_USED2 = "lastUsed2";
    private static final String LAST_USED2_URL = "lastUsedUrl2";
    private static final String LAST_USED3 = "lastUsed3";
    private static final String LAST_USED3_URL = "lastUsedUrl3";
    private static final String LAST_USED4 = "lastUsed4";
    private static final String LAST_USED4_URL = "lastUsedUrl4";
    private static final String WorkbenchInfoResourceName =
        "mondrian.gui.resources.workbenchInfo";
    private static final String GUIResourceName = "mondrian.gui.resources.gui";
    private static final String TextResourceName =
        "mondrian.gui.resources.text";
    private static final String FILTER_SCHEMA_LIST = "FILTER_SCHEMA_LIST";

    private static final Logger LOGGER = Logger.getLogger(Workbench.class);

    private String jdbcDriverClassName;
    private String jdbcConnectionUrl;
    private String jdbcUsername;
    private String jdbcPassword;
    private String jdbcSchema;
    private boolean requireSchema;

    private JdbcMetaData jdbcMetaData;

    private final ClassLoader myClassLoader;

    private Properties workbenchProperties;
    private static ResourceBundle workbenchResourceBundle = null;

    private static I18n resourceConverter = null;

    private static int newSchema = 1;

    private String openFile = null;

    private Map<JInternalFrame, JMenuItem> schemaWindowMap =
        new HashMap<JInternalFrame, JMenuItem>();

    private final List<JInternalFrame> mdxWindows =
        new ArrayList<JInternalFrame>();
    private final List<JInternalFrame> jdbcWindows =
        new ArrayList<JInternalFrame>();
    private int windowMenuMapIndex = 1;

    private static final String KETTLE_PLUGIN_BASE_FOLDERS = "kettle-plugins,"
            + Const.getKettleDirectory() + Const.FILE_SEPARATOR + "plugins";
    private XulDialog connectionDialog = null;
    private DataHandler connectionDialogController = null;
    private DatabaseMeta dbMeta = null;

    /**
     * Creates new form Workbench
     */
    public Workbench() {
        myClassLoader = this.getClass().getClassLoader();

        resourceConverter = getGlobalResourceConverter();

        // Setting User home directory
        WORKBENCH_USER_HOME_DIR =
            System.getProperty("user.home")
            + File.separator
            + ".schemaWorkbench";
        WORKBENCH_CONFIG_FILE =
            WORKBENCH_USER_HOME_DIR + File.separator + "workbench.properties";
        DB_META_CONFIG_FILE = WORKBENCH_USER_HOME_DIR + File.separator
                + "databaseMeta.xml";

        loadWorkbenchProperties();
        loadDatabaseMeta();
        initOptions();
        initComponents();
        loadMenubarPlugins();

        ImageIcon icon = new javax.swing.ImageIcon(
            myClassLoader.getResource(
                getResourceConverter().getGUIReference("productIcon")));

        this.setIconImage(icon.getImage());
    }

    public static I18n getGlobalResourceConverter() {
        if (resourceConverter == null) {
            ClassLoader currentClassLoader;

            ResourceBundle localGuiResourceBundle;
            ResourceBundle localTextResourceBundle;

            currentClassLoader = Workbench.class.getClassLoader();

            localGuiResourceBundle = ResourceBundle.getBundle(
                GUIResourceName, Locale.getDefault(), currentClassLoader);
            localTextResourceBundle = ResourceBundle.getBundle(
                TextResourceName, Locale.getDefault(), currentClassLoader);

            resourceConverter = new I18n(
                localGuiResourceBundle, localTextResourceBundle);
        }
        return resourceConverter;
    }

    /**
     * load properties
     */
    private void loadWorkbenchProperties() {
        workbenchProperties = new Properties();
        try {
            workbenchResourceBundle = ResourceBundle.getBundle(
                WorkbenchInfoResourceName, Locale.getDefault(),
                    myClassLoader);
            File f = new File(WORKBENCH_CONFIG_FILE);
            if (f.exists()) {
                workbenchProperties.load(new FileInputStream(f));
            } else {
                LOGGER.debug(WORKBENCH_CONFIG_FILE + " does not exist");
            }
        } catch (Exception e) {
            // TODO deal with exception
            LOGGER.error("loadWorkbenchProperties", e);
        }
    }

    /**
     * load database meta
     */
    public void loadDatabaseMeta() {
        if (dbMeta == null) {
            File file = new File(DB_META_CONFIG_FILE);
            if (file.exists()) {
                try {
                    final String fileContents =
                        FileUtils.readFileToString(file);
                    if (Util.isBlank(fileContents)) {
                        LOGGER.error(
                            "DB Meta file is empty at: "
                            + DB_META_CONFIG_FILE);
                    } else {
                        dbMeta = getDbMeta(fileContents);
                    }
                } catch (Exception e) {
                    LOGGER.error(
                        "Failed to load DB meta file at: "
                        + DB_META_CONFIG_FILE,
                        e);
                }
            }
        }

        if (dbMeta != null) {
            syncToWorkspace(dbMeta);
        }
    }

    /**
     * returns the value of a workbench property
     *
     * @param key key to lookup
     * @return the value
     */
    public String getWorkbenchProperty(String key) {
        return workbenchProperties.getProperty(key);
    }

    /**
     * set a workbench property.  Note that this does not save the property,
     * a call to storeWorkbenchProperties is required.
     *
     * @param key   property key
     * @param value property value
     */
    public void setWorkbenchProperty(String key, String value) {
        workbenchProperties.setProperty(key, value);
    }

    /**
     * save properties
     */
    public void storeWorkbenchProperties() {
        // save properties to file
        File dir = new File(WORKBENCH_USER_HOME_DIR);
        try {
            if (dir.exists()) {
                if (!dir.isDirectory()) {
                    JOptionPane.showMessageDialog(
                        this,
                        getResourceConverter().getFormattedString(
                            "workbench.user.home.not.directory",
                            "{0} is not a directory!\nPlease rename this file and retry to save configuration!",
                            WORKBENCH_USER_HOME_DIR),
                        "",
                        JOptionPane.ERROR_MESSAGE);
                    return;
                }
            } else {
                dir.mkdirs();
            }
        } catch (Exception ex) {
            LOGGER.error("storeWorkbenchProperties: mkdirs", ex);
            JOptionPane.showMessageDialog(
                this,
                getResourceConverter().getFormattedString(
                    "workbench.user.home.exception",
                    "An error is occurred creating workbench configuration directory:\n{0}\nError is: {1}",
                    WORKBENCH_USER_HOME_DIR,
                    ex.getLocalizedMessage()),
                "",
                JOptionPane.ERROR_MESSAGE);
            return;
        }

        OutputStream out = null;
        try {
            out = new FileOutputStream(
                new File(
                    WORKBENCH_CONFIG_FILE));
            workbenchProperties.store(out, "Workbench configuration");
        } catch (Exception e) {
            LOGGER.error("storeWorkbenchProperties: store", e);
            JOptionPane.showMessageDialog(
                this,
                getResourceConverter().getFormattedString(
                    "workbench.save.configuration",
                    "An error is occurred creating workbench configuration file:\n{0}\nError is: {1}",
                    WORKBENCH_CONFIG_FILE,
                    e.getLocalizedMessage()),
                "",
                JOptionPane.ERROR_MESSAGE);
        } finally {
            try {
                out.close();
            } catch (IOException eIO) {
                LOGGER.error("storeWorkbenchProperties: out.close", eIO);
            }
        }
    }

    /**
     * save database meta
     */
    public void storeDatabaseMeta() {
        if (dbMeta != null) {
            try {
                File file = new File(DB_META_CONFIG_FILE);
                PrintWriter pw = new PrintWriter(new FileWriter(file));
                pw.println(dbMeta.getXML());
                pw.close();
            } catch (IOException e) {
                LOGGER.error("storeDatabaseMeta", e);
            }
        }
    }

    /**
     * Initialize the UI options
     */
    private void initOptions() {
        requireSchema = "true".equals(getWorkbenchProperty("requireSchema"));
    }

    /**
     * This method is called from within the constructor to
     * initialize the form.
     */
    private void initComponents() {
        desktopPane = new javax.swing.JDesktopPane();
        jToolBar1 = new javax.swing.JToolBar();
        jToolBar2 = new javax.swing.JToolBar();
        toolbarNewPopupMenu = new JPopupMenu();
        toolbarNewButton = new javax.swing.JButton();
        toolbarOpenButton = new javax.swing.JButton();
        toolbarSaveButton = new javax.swing.JButton();
        toolbarSaveAsButton = new javax.swing.JButton();
        jPanel1 = new javax.swing.JPanel();
        jPanel2 = new javax.swing.JPanel();
        toolbarPreferencesButton = new javax.swing.JButton();
        requireSchemaCheckboxMenuItem = new javax.swing.JCheckBoxMenuItem();
        menuBar = new javax.swing.JMenuBar();
        fileMenu = new javax.swing.JMenu();
        newMenu = new javax.swing.JMenu();
        newSchemaMenuItem = new javax.swing.JMenuItem();
        newQueryMenuItem = new javax.swing.JMenuItem();
        newJDBCExplorerMenuItem = new javax.swing.JMenuItem();
        newSchemaMenuItem2 = new javax.swing.JMenuItem();
        newQueryMenuItem2 = new javax.swing.JMenuItem();
        newJDBCExplorerMenuItem2 = new javax.swing.JMenuItem();
        openMenuItem = new javax.swing.JMenuItem();
        preferencesMenuItem = new javax.swing.JMenuItem();
        lastUsed1MenuItem = new javax.swing.JMenuItem();
        lastUsed2MenuItem = new javax.swing.JMenuItem();
        lastUsed3MenuItem = new javax.swing.JMenuItem();
        lastUsed4MenuItem = new javax.swing.JMenuItem();
        saveMenuItem = new javax.swing.JMenuItem();
        saveAsMenuItem = new javax.swing.JMenuItem();
        jSeparator1 = new javax.swing.JSeparator();
        jSeparator2 = new javax.swing.JSeparator();
        jSeparator3 = new javax.swing.JSeparator();
        exitMenuItem = new javax.swing.JMenuItem();
        windowMenu = new javax.swing.JMenu();
        helpMenu = new javax.swing.JMenu();
        editMenu = new javax.swing.JMenu();
        cutMenuItem =
            new javax.swing.JMenuItem(new DefaultEditorKit.CutAction());
        copyMenuItem =
            new javax.swing.JMenuItem(new DefaultEditorKit.CopyAction());
        pasteMenuItem =
            new javax.swing.JMenuItem(new DefaultEditorKit.PasteAction());
        deleteMenuItem =
            new javax.swing.JMenuItem(
                new AbstractAction(
                    getResourceConverter().getString(
                        "workbench.menu.delete", "Delete"))
                {
                    private static final long serialVersionUID = 1L;
                    public void actionPerformed(ActionEvent e) {
                        JInternalFrame jf = desktopPane.getSelectedFrame();
                        if (jf != null && jf.getContentPane()
                            .getComponent(0) instanceof SchemaExplorer)
                        {
                            SchemaExplorer se =
                                (SchemaExplorer) jf.getContentPane()
                                    .getComponent(0);
                            TreePath tpath = se.tree.getSelectionPath();
                            se.delete(tpath);
                        }
                    }
                });
        aboutMenuItem = new javax.swing.JMenuItem();
        toolsMenu = new javax.swing.JMenu();
        viewMenu = new javax.swing.JMenu();
        viewXmlMenuItem = new javax.swing.JCheckBoxMenuItem();

        setTitle(
            getResourceConverter().getString(
                "workbench.panel.title", "Schema Workbench"));
        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(
            new WindowAdapter() {
                public void windowClosing(WindowEvent evt) {
                    storeWorkbenchProperties();
                    storeDatabaseMeta();
                    closeAllSchemaFrames(true);
                }
            });

        getContentPane().add(desktopPane, java.awt.BorderLayout.CENTER);


        newSchemaMenuItem2.setText(
            getResourceConverter().getString(
                "workbench.menu.newSchema", "Schema"));
        newSchemaMenuItem2.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    newSchemaMenuItemActionPerformed(evt);
                }
            });

        newQueryMenuItem2.setText(
            getResourceConverter().getString(
                "workbench.menu.newQuery", "MDX Query"));
        newQueryMenuItem2.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    newQueryMenuItemActionPerformed(evt);
                }
            });


        newJDBCExplorerMenuItem2.setText(
            getResourceConverter().getString(
                "workbench.menu.newJDBC", "JDBC Explorer"));
        newJDBCExplorerMenuItem2.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    newJDBCExplorerMenuItemActionPerformed(evt);
                }
            });


        toolbarNewPopupMenu.add(newSchemaMenuItem2);
        toolbarNewPopupMenu.add(newQueryMenuItem2);
        toolbarNewPopupMenu.add(newJDBCExplorerMenuItem2);


        jPanel2.setLayout(new java.awt.BorderLayout());
        jPanel2.setBorder(javax.swing.BorderFactory.createEtchedBorder());
        jPanel2.setMaximumSize(new java.awt.Dimension(50, 28));

        toolbarNewButton.setIcon(
            new javax.swing.ImageIcon(
                getClass().getResource(
                    getResourceConverter().getGUIReference("new"))));
        toolbarNewButton.setToolTipText(
            getResourceConverter().getString(
                "workbench.toolbar.new", "New"));
        toolbarNewButton.setBorderPainted(false);
        toolbarNewButton.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent e) {
                    toolbarNewPopupMenu.show(
                        jPanel2, 0, jPanel2.getSize().height);
                }
            });

        jToolBar2.setFloatable(false);
        jToolBar2.add(toolbarNewButton);

        jPanel2.add(jToolBar2, java.awt.BorderLayout.CENTER);

        toolbarNewArrowButton = new BasicArrowButton(SwingConstants.SOUTH);
        toolbarNewArrowButton.setToolTipText(
            getResourceConverter().getString(
                "workbench.toolbar.newArrow", "New"));
        toolbarNewArrowButton.setBorderPainted(false);
        toolbarNewArrowButton.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent e) {
                    toolbarNewPopupMenu.show(
                        jPanel2, 0, jPanel2.getSize().height);
                }
            });

        jPanel2.add(toolbarNewArrowButton, java.awt.BorderLayout.EAST);

        jToolBar1.add(jPanel2, 0);


        toolbarOpenButton.setIcon(
            new javax.swing.ImageIcon(
                getClass().getResource(
                    getResourceConverter().getGUIReference("open"))));
        toolbarOpenButton.setToolTipText(
            getResourceConverter().getString(
                "workbench.toolbar.open", "Open"));
        toolbarOpenButton.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    openMenuItemActionPerformed(evt);
                }
            });

        jToolBar1.add(toolbarOpenButton);


        toolbarSaveButton.setIcon(
            new javax.swing.ImageIcon(
                getClass().getResource(
                    getResourceConverter().getGUIReference("save"))));
        toolbarSaveButton.setToolTipText(
            getResourceConverter().getString(
                "workbench.toolbar.save", "Save"));
        toolbarSaveButton.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    saveMenuItemActionPerformed(evt);
                }
            });

        jToolBar1.add(toolbarSaveButton);

        toolbarSaveAsButton.setIcon(
            new javax.swing.ImageIcon(
                getClass().getResource(
                    getResourceConverter().getGUIReference("saveAs"))));
        toolbarSaveAsButton.setToolTipText(
            getResourceConverter().getString(
                "workbench.toolbar.saveAs", "Save As"));
        toolbarSaveAsButton.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    saveAsMenuItemActionPerformed(evt);
                }
            });

        jToolBar1.add(toolbarSaveAsButton);

        jPanel1.setMaximumSize(new java.awt.Dimension(8, 8));
        jToolBar1.add(jPanel1);

        toolbarPreferencesButton.setIcon(
            new javax.swing.ImageIcon(
                getClass().getResource(
                    getResourceConverter().getGUIReference("preferences"))));
        toolbarPreferencesButton.setToolTipText(
            getResourceConverter().getString(
                "workbench.toolbar.connection", "Connection"));
        toolbarPreferencesButton.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    connectionButtonActionPerformed(evt);
                }
            });

        jToolBar1.add(toolbarPreferencesButton);


        getContentPane().add(jToolBar1, java.awt.BorderLayout.NORTH);

        fileMenu.setText(
            getResourceConverter().getString(
                "workbench.menu.file", "File"));
        fileMenu.setMnemonic(KeyEvent.VK_F);
        newMenu.setText(
            getResourceConverter().getString(
                "workbench.menu.new", "New"));

        newSchemaMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.newSchema", "Schema"));
        newSchemaMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    newSchemaMenuItemActionPerformed(evt);
                }
            });

        newMenu.add(newSchemaMenuItem);

        newQueryMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.newQuery", "MDX Query"));
        newQueryMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    newQueryMenuItemActionPerformed(evt);
                }
            });

        newMenu.add(newQueryMenuItem);

        newJDBCExplorerMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.newJDBC", "JDBC Explorer"));
        newJDBCExplorerMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    newJDBCExplorerMenuItemActionPerformed(evt);
                }
            });

        newMenu.add(newJDBCExplorerMenuItem);

        fileMenu.add(newMenu);

        openMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.open", "Open"));
        openMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    openMenuItemActionPerformed(evt);
                }
            });

        fileMenu.add(openMenuItem);

        saveMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.save", "Save"));
        saveMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    saveMenuItemActionPerformed(evt);
                }
            });

        fileMenu.add(saveMenuItem);

        saveAsMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.saveAsDot", "Save As ..."));
        saveAsMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    saveAsMenuItemActionPerformed(evt);
                }
            });

        fileMenu.add(saveAsMenuItem);

        // add last used
        fileMenu.add(jSeparator2);

        lastUsed1MenuItem.setText(getWorkbenchProperty("lastUsed1"));
        lastUsed1MenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    lastUsed1MenuItemActionPerformed(evt);
                }
            });
        fileMenu.add(lastUsed1MenuItem);

        lastUsed2MenuItem.setText(getWorkbenchProperty("lastUsed2"));
        lastUsed2MenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    lastUsed2MenuItemActionPerformed(evt);
                }
            });
        fileMenu.add(lastUsed2MenuItem);

        lastUsed3MenuItem.setText(getWorkbenchProperty("lastUsed3"));
        lastUsed3MenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    lastUsed3MenuItemActionPerformed(evt);
                }
            });
        fileMenu.add(lastUsed3MenuItem);

        lastUsed4MenuItem.setText(getWorkbenchProperty("lastUsed4"));
        lastUsed4MenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    lastUsed4MenuItemActionPerformed(evt);
                }
            });
        fileMenu.add(lastUsed4MenuItem);

        updateLastUsedMenu();
        fileMenu.add(jSeparator1);

        exitMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.exit", "Exit"));
        exitMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    exitMenuItemActionPerformed(evt);
                }
            });

        fileMenu.add(exitMenuItem);

        menuBar.add(fileMenu);

        editMenu.setText(
            getResourceConverter().getString(
                "workbench.menu.edit", "Edit"));
        editMenu.setMnemonic(KeyEvent.VK_E);
        cutMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.cut", "Cut"));
        editMenu.add(cutMenuItem);

        copyMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.copy", "Copy"));
        editMenu.add(copyMenuItem);

        pasteMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.paste", "Paste"));
        editMenu.add(pasteMenuItem);

        editMenu.add(deleteMenuItem);

        menuBar.add(editMenu);

        viewMenu.setText(
            getResourceConverter().getString(
                "workbench.menu.view", "View"));
        viewMenu.setMnemonic(KeyEvent.VK_V);
        viewXmlMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.viewXML", "View XML"));
        viewXmlMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    viewXMLMenuItemActionPerformed(evt);
                }
            });
        viewMenu.add(viewXmlMenuItem);
        menuBar.add(viewMenu);

        toolsMenu.setText(getResourceConverter().getString(
            "workbench.menu.options", "Options"));
        toolsMenu.setMnemonic(KeyEvent.VK_O);
        preferencesMenuItem.setText(getResourceConverter().getString(
            "workbench.menu.connection", "Connection"));
        preferencesMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    connectionButtonActionPerformed(evt);
                }
            });
        toolsMenu.add(preferencesMenuItem);

        requireSchemaCheckboxMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.requireSchema", "Require Schema"));
        requireSchemaCheckboxMenuItem.setSelected(requireSchema);
        requireSchemaCheckboxMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent e) {
                    requireSchemaActionPerformed(e);
                }
            });

        toolsMenu.add(requireSchemaCheckboxMenuItem);
        menuBar.add(toolsMenu);


        windowMenu.setText(
            getResourceConverter().getString(
                "workbench.menu.windows", "Windows"));
        windowMenu.setMnemonic(KeyEvent.VK_W);

        cascadeMenuItem = new javax.swing.JMenuItem();
        cascadeMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.cascadeWindows", "Cascade Windows"));
        cascadeMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    cascadeMenuItemActionPerformed(evt);
                }
            });

        tileMenuItem = new javax.swing.JMenuItem();
        tileMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.tileWindows", "Tile Windows"));
        tileMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    tileMenuItemActionPerformed(evt);
                }
            });

        closeAllMenuItem = new javax.swing.JMenuItem();
        closeAllMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.closeAll", "Close All"));
        closeAllMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    closeAllMenuItemActionPerformed(evt);
                }
            });

        minimizeMenuItem = new javax.swing.JMenuItem();
        minimizeMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.minimizeAll", "Minimize All"));
        minimizeMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    minimizeMenuItemActionPerformed(evt);
                }
            });

        maximizeMenuItem = new javax.swing.JMenuItem();
        maximizeMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.maximizeAll", "Maximize All"));
        maximizeMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    maximizeMenuItemActionPerformed(evt);
                }
            });

        menuBar.add(windowMenu);

        aboutMenuItem.setText(
            getResourceConverter().getString(
                "workbench.menu.about", "About"));
        aboutMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    aboutMenuItemActionPerformed(evt);
                }
            });

        helpMenu.add(aboutMenuItem);

        helpMenu.setText(
            getResourceConverter().getString(
                "workbench.menu.help", "Help"));
        helpMenu.setMnemonic(KeyEvent.VK_H);
        menuBar.add(helpMenu);

        setJMenuBar(menuBar);

        pack();
    }

    /**
     * this method loads any available menubar plugins based on
     */
    private void loadMenubarPlugins() {
        // render any plugins
        InputStream pluginStream = null;
        try {
            Properties props = new Properties();
            pluginStream = getClass().getResourceAsStream(
                "/workbench_plugins.properties");
            if (pluginStream != null) {
                props.load(pluginStream);
                for (Object key : props.keySet()) {
                    String keystr = (String) key;
                    if (keystr.startsWith("workbench.menu-plugin")) {
                        String val = props.getProperty(keystr);
                        WorkbenchMenubarPlugin plugin =
                            (WorkbenchMenubarPlugin) Class.forName(val)
                                .newInstance();
                        plugin.setWorkbench(this);
                        plugin.addItemsToMenubar(menuBar);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (pluginStream != null) {
                    pluginStream.close();
                }
            } catch (Exception e) {
            }
        }
    }


    /**
     * @return the workbenchResourceBundle
     */
    public ResourceBundle getWorkbenchResourceBundle() {
        return workbenchResourceBundle;
    }

    /**
     * @return the resourceConverter
     */
    public I18n getResourceConverter() {
        if (resourceConverter == null) {
            resourceConverter = getGlobalResourceConverter();
        }
        return resourceConverter;
    }

    private void tileMenuItemActionPerformed(ActionEvent evt) {
        final Dimension dsize = desktopPane.getSize();
        final int desktopW = (int) dsize.getWidth();
        final int desktopH = (int) dsize.getHeight();
        final int darea = desktopW * desktopH;
        final double eacharea =
            darea
            / (schemaWindowMap.size() + mdxWindows.size() + jdbcWindows.size());
        final int wh = (int) Math.sqrt(eacharea);

        try {
            int x = 0, y = 0;
            for (JInternalFrame sf : getAllFrames()) {
                if (sf != null && !sf.isIcon()) {
                    sf.setMaximum(false);
                    sf.moveToFront();
                    if (x >= desktopW
                        || (desktopW - x) * wh < eacharea / 2)
                    {
                        // move to next row of windows
                        y += wh;
                        x = 0;
                    }
                    int sfwidth = ((x + wh) < desktopW
                        ? wh
                        : desktopW - x);
                    int sfheight = ((y + wh) < desktopH
                        ? wh
                        : desktopH - y);
                    sf.setBounds(x, y, sfwidth, sfheight);
                    x += sfwidth;
                }
            }
        } catch (Exception ex) {
            LOGGER.error("tileMenuItemActionPerformed", ex);
            // do nothing
        }
    }

    // cascade all the indows open in schema workbench
    private void cascadeMenuItemActionPerformed(
        ActionEvent evt)
    {
        try {
            int sfi = 1;
            for (JInternalFrame sf : getAllFrames()) {
                if (sf != null && !sf.isIcon()) {
                    sf.setMaximum(false);
                    sf.setLocation(30 * sfi, 30 * sfi);
                    sf.moveToFront();
                    sf.setSelected(true);
                    sfi++;
                }
            }
        } catch (Exception ex) {
            LOGGER.error("cascadeMenuItemActionPerformed", ex);
            // do nothing
        }
    }

    // close all the windows open in schema workbench
    private void closeAllMenuItemActionPerformed(ActionEvent evt) {
        closeAllSchemaFrames(false);
    }

    private void closeAllSchemaFrames(boolean exitAfterClose) {
        try {
            for (JInternalFrame sf : getAllFrames()) {
                if (sf == null) {
                    continue;
                }
                if (sf.getContentPane().getComponent(0)
                    instanceof SchemaExplorer)
                {
                    SchemaExplorer se =
                        (SchemaExplorer) sf.getContentPane().getComponent(0);
                    sf.setSelected(true);
                    int response = confirmFrameClose(sf, se);
                    switch (response) {
                    case 2:
                        // cancel
                        return;
                    case 3:
                        // not dirty
                        sf.setClosed(true);
                        break;
                    }
                } else {
                    sf.setClosed(true);
                }
            }
            // exit Schema Workbench if no files are open
            if (((schemaWindowMap.keySet().size()) == 0) && exitAfterClose) {
                System.exit(0);
            }
        } catch (Exception ex) {
            LOGGER.error("closeAllSchemaFrames", ex);
        }
    }

    private int confirmFrameClose(
        JInternalFrame schemaFrame,
        SchemaExplorer se)
    {
        if (se.isDirty()) {
            JMenuItem schemaMenuItem = schemaWindowMap.get(
                desktopPane.getSelectedFrame());
            // yes=0; no=1; cancel=2
            int answer = JOptionPane.showConfirmDialog(
                null,
                getResourceConverter().getFormattedString(
                    "workbench.saveSchemaOnClose.alert",
                    "Save changes to {0}?",
                    se.getSchemaFile().toString()),
                getResourceConverter().getString(
                    "workbench.saveSchemaOnClose.title", "Schema"),
                JOptionPane.YES_NO_CANCEL_OPTION);
            switch (answer) {
            case 0:
                saveMenuItemActionPerformed(null);
                schemaWindowMap.remove(schemaFrame);
                updateMDXCatalogList();
                schemaFrame.dispose();
                windowMenu.remove(schemaMenuItem);
                break;
            case 1:
                schemaFrame.dispose();
                schemaWindowMap.remove(schemaFrame);
                windowMenu.remove(schemaMenuItem);
                break;
            case 2:
                try {
                    schemaFrame.setClosed(false);
                    schemaFrame.show();
                } catch (Exception ex) {
                    LOGGER.error(ex);
                }
            }
            return answer;
        }
        return 3;
    }

    private void minimizeMenuItemActionPerformed(
        ActionEvent evt)
    {
        try {
            for (JInternalFrame sf : getAllFrames()) {
                if (sf != null && !sf.isIcon()) {
                    sf.setIcon(true);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("minimizeMenuItemActionPerformed", ex);
            // do nothing
        }
    }

    private void maximizeMenuItemActionPerformed(
        ActionEvent evt)
    {
        try {
            for (JInternalFrame sf : getAllFrames()) {
                if (sf != null) {
                    sf.setIcon(false);
                    sf.setMaximum(true);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("maximizeMenuItemActionPerformed", ex);
            // do nothing
        }
    }

    /**
     * Returns an iterable over all internal frames.
     */
    @SuppressWarnings("unchecked")
    private Iterable<JInternalFrame> getAllFrames() {
        return UnionIterator.over(
            schemaWindowMap.keySet(), mdxWindows, jdbcWindows);
    }

    private void aboutMenuItemActionPerformed(ActionEvent evt) {
        try {
            JEditorPane jEditorPane =
                new JEditorPane(
                    myClassLoader.getResource(
                        getResourceConverter().getGUIReference("version"))
                        .toString());
            jEditorPane.setEditable(false);
            JScrollPane jScrollPane = new JScrollPane(jEditorPane);
            JPanel jPanel = new JPanel();
            jPanel.setLayout(new java.awt.BorderLayout());
            jPanel.add(jScrollPane, java.awt.BorderLayout.CENTER);

            JInternalFrame jf = new JInternalFrame();
            jf.setTitle("About");
            jf.getContentPane().add(jPanel);

            Dimension screenSize = this.getSize();
            int aboutW = 400;
            int aboutH = 300;
            int width = (screenSize.width / 2) - (aboutW / 2);
            int height = (screenSize.height / 2) - (aboutH / 2) - 100;
            jf.setBounds(width, height, aboutW, aboutH);
            jf.setClosable(true);

            desktopPane.add(jf);

            jf.setVisible(true);
            jf.show();
        } catch (Exception ex) {
            LOGGER.error("aboutMenuItemActionPerformed", ex);
        }
    }

    private void newJDBCExplorerMenuItemActionPerformed(
        ActionEvent evt)
    {
        try {
            if (jdbcMetaData == null) {
                getNewJdbcMetadata();
            }

            final JInternalFrame jf = new JInternalFrame();

            jf.setTitle(
                getResourceConverter().getFormattedString(
                    "workbench.new.JDBCExplorer.title",
                    "JDBC Explorer - {0} {1}",
                    jdbcMetaData.getDatabaseProductName(),
                    jdbcMetaData.getJdbcConnectionUrl()));
            getNewJdbcMetadata();

            JdbcExplorer jdbce = new JdbcExplorer(jdbcMetaData, this);

            jf.getContentPane().add(jdbce);
            jf.setBounds(0, 0, 500, 480);
            jf.setClosable(true);
            jf.setIconifiable(true);
            jf.setMaximizable(true);
            jf.setResizable(true);
            jf.setVisible(true);

            // create jdbc menu item
            final javax.swing.JMenuItem jdbcMenuItem =
                new javax.swing.JMenuItem();
            jdbcMenuItem.setText(
                getResourceConverter().getFormattedString(
                    "workbench.new.JDBCExplorer.menuitem",
                    "{0} JDBC Explorer",
                    Integer.toString(windowMenuMapIndex++)));
            jdbcMenuItem.addActionListener(
                new ActionListener() {
                    public void actionPerformed(ActionEvent evt) {
                        try {
                            if (jf.isIcon()) {
                                jf.setIcon(false);
                            } else {
                                jf.setSelected(true);
                            }
                        } catch (Exception ex) {
                            LOGGER.error("queryMenuItem", ex);
                        }
                    }
                });

            jf.addInternalFrameListener(
                new InternalFrameAdapter() {
                    public void internalFrameClosing(InternalFrameEvent e) {
                        jdbcWindows.remove(jf);
                        jf.dispose();
                        // follow this by removing file from schemaWindowMap
                        windowMenu.remove(jdbcMenuItem);
                        return;
                    }
                });

            desktopPane.add(jf);
            jf.setVisible(true);
            jf.show();

            try {
                jf.setSelected(true);
            } catch (Exception ex) {
                // do nothing
                LOGGER.error(
                    "newJDBCExplorerMenuItemActionPerformed.setSelected", ex);
            }

            jdbcWindows.add(jf);

            windowMenu.add(jdbcMenuItem, -1);
            windowMenu.add(jSeparator3, -1);
            windowMenu.add(cascadeMenuItem, -1);
            windowMenu.add(tileMenuItem, -1);
            windowMenu.add(minimizeMenuItem, -1);
            windowMenu.add(maximizeMenuItem, -1);
            windowMenu.add(closeAllMenuItem, -1);
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(
                this,
                getResourceConverter().getFormattedString(
                    "workbench.new.JDBCExplorer.exception",
                    "Database connection not successful.\n{0}",
                    ex.getLocalizedMessage()),
                getResourceConverter().getString(
                    "workbench.new.JDBCExplorer.exception.title",
                    "Database Connection Error"),
                JOptionPane.ERROR_MESSAGE);
            LOGGER.error("newJDBCExplorerMenuItemActionPerformed", ex);
        }
    }

    /**
     * Convenience method for retrieving the dbMeta instance that handles
     * required Kettle initialization.
     *
     * @param xml output of {@link DatabaseMeta#getXML()} or <code>null</code>
     * @return the current {@link DatabaseMeta} instance
     */
    private DatabaseMeta getDbMeta(String xml) {
        try {
            if (!KettleClientEnvironment.isInitialized()) {
                System.setProperty(
                    "KETTLE_PLUGIN_BASE_FOLDERS",
                    KETTLE_PLUGIN_BASE_FOLDERS);
                KettleClientEnvironment.init();
            }
            if (dbMeta != null) {
                return dbMeta;
            }
            if (xml == null) {
                dbMeta = new DatabaseMeta();
            } else {
                dbMeta = new DatabaseMeta(xml);
            }
        } catch (KettleException e) {
            throw new RuntimeException(
                getResourceConverter().getFormattedString(
                    "workbench.new.Kettle.exception",
                    "Kettle failed to initialize."),
                e);
        }
        return dbMeta;
    }

    private void connectionButtonActionPerformed(ActionEvent evt) {
        if (connectionDialog == null) {
            dbMeta = getDbMeta(null);
            connectionDialogController = new DataHandler();
            connectionDialogController.setName("dataHandler");

            XulDomContainer container = null;

            try {
                XulLoader loader = new SwingXulLoader();
                container = loader.loadXul(
                    DatabaseConnectionDialog.DIALOG_DEFINITION_FILE,
                    Messages.getBundle());
            } catch (XulException e) {
                throw new RuntimeException("Xul failed to initialize", e);
            }
            container.addEventHandler(connectionDialogController);
            connectionDialogController.loadConnectionData();
            connectionDialogController.setData(dbMeta);
            connectionDialog = (XulDialog) container.getDocumentRoot()
                    .getRootElement();
        }

        connectionDialog.show();

        dbMeta = (DatabaseMeta) connectionDialogController.getData();
        if (dbMeta.hasChanged()) {
            dbMeta.clearChanged();
            syncToWorkspace(dbMeta);
            // Enforces the JDBC preferences entered throughout all schemas
            // currently opened in the workbench.
            resetWorkbench();
        }
    }

    private void syncToWorkspace(DatabaseMeta databaseMeta) {
        // sync from dbmeta to wkspc
        try {
            jdbcConnectionUrl = databaseMeta.getURL();
        } catch (KettleDatabaseException e) {
            throw new RuntimeException("Failed to determine JDBC URL", e);
        }
        jdbcDriverClassName = databaseMeta.getDriverClass();
        jdbcUsername = databaseMeta.getUsername();
        jdbcPassword = databaseMeta.getPassword();
        //jdbcSchema = databaseMeta.getPreferredSchemaName();
        Map<String, String> options = dbMeta.getExtraOptions();

        String dbType = dbMeta.getDatabaseInterface().getPluginId();
        jdbcSchema = options.get(dbType + "." + FILTER_SCHEMA_LIST);

        // saving to workbench properties for documentation purposes only, since
        // persistence
        // of the dbmeta object is handled by a separate xml file now
        if (jdbcDriverClassName != null) {
            setWorkbenchProperty("jdbcDriverClassName", jdbcDriverClassName);
        }
        if (jdbcConnectionUrl != null) {
            setWorkbenchProperty("jdbcConnectionUrl", jdbcConnectionUrl);
        }
        if (jdbcUsername != null) {
            setWorkbenchProperty("jdbcUsername", jdbcUsername);
        }
        if (jdbcPassword != null) {
            setWorkbenchProperty("jdbcPassword", jdbcPassword);
        }
        if (jdbcSchema != null) {
            setWorkbenchProperty("jdbcSchema", jdbcSchema);
        }
    }

    private void requireSchemaActionPerformed(ActionEvent evt) {
        requireSchema = ((JCheckBoxMenuItem) evt.getSource()).isSelected();
        setWorkbenchProperty("requireSchema", "" + requireSchema);
    }


    private void newSchemaMenuItemActionPerformed(ActionEvent evt) {
        MondrianProperties.instance();
        // User's default directory. This default depends on the operating
        // system.  It is typically the "My Documents" folder on Windows, and
        // the user's home directory on Unix.
        File defaultDir =
            FileSystemView.getFileSystemView().getDefaultDirectory();
        File outputFile;
        do {
            outputFile = new File(defaultDir, "Schema" + newSchema++ + ".xml");
        } while (outputFile.exists());

        openSchemaFrame(outputFile, true);
    }

    private void newQueryMenuItemActionPerformed(ActionEvent evt) {
        JMenuItem schemaMenuItem =
            schemaWindowMap.get(desktopPane.getSelectedFrame());

        final JInternalFrame jf = new JInternalFrame();
        jf.setTitle(
            getResourceConverter().getString(
                "workbench.new.MDXQuery.title", "MDX Query"));
        QueryPanel qp = new QueryPanel(this);

        jf.getContentPane().add(qp);
        jf.setBounds(0, 0, 500, 480);
        jf.setClosable(true);
        jf.setIconifiable(true);
        jf.setMaximizable(true);
        jf.setResizable(true);
        jf.setVisible(true);

        desktopPane.add(jf);
        jf.show();
        try {
            jf.setSelected(true);
        } catch (Exception ex) {
            // do nothing
            LOGGER.error("newQueryMenuItemActionPerformed.setSelected", ex);
        }

        // add the mdx frame to this set of mdx frames for cascading method
        mdxWindows.add(jf);

        // create mdx menu item
        final javax.swing.JMenuItem queryMenuItem = new javax.swing.JMenuItem();
        queryMenuItem.setText(
            getResourceConverter().getFormattedString(
                "workbench.new.MDXQuery.menuitem",
                "{0} MDX",
                Integer.toString(windowMenuMapIndex)));
        queryMenuItem.addActionListener(
            new ActionListener() {
                public void actionPerformed(ActionEvent evt) {
                    try {
                        if (jf.isIcon()) {
                            jf.setIcon(false);
                        } else {
                            jf.setSelected(true);
                        }
                    } catch (Exception ex) {
                        LOGGER.error("queryMenuItem", ex);
                    }
                }
            });

        // disable mdx frame close operation to provide our handler
        // to remove frame object from mdxframeset before closing
        jf.setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);

        jf.addInternalFrameListener(
            new InternalFrameAdapter() {
                public void internalFrameClosing(InternalFrameEvent e) {
                    mdxWindows.remove(jf);
                    jf.dispose();
                    // follow this by removing file from schemaWindowMap
                    windowMenu.remove(queryMenuItem);
                    return;
                }
            });

        windowMenu.add(queryMenuItem, -1);
        windowMenu.add(jSeparator3, -1);
        windowMenu.add(cascadeMenuItem, -1);
        windowMenu.add(tileMenuItem, -1);
        windowMenu.add(minimizeMenuItem, -1);
        windowMenu.add(maximizeMenuItem, -1);
        windowMenu.add(closeAllMenuItem, -1);

        qp.setMenuItem(queryMenuItem);
        qp.setSchemaWindowMap(schemaWindowMap);
        qp.setWindowMenuIndex(windowMenuMapIndex++);

        if (schemaMenuItem != null) {
            qp.initConnection(schemaMenuItem.getText());
        } else {
            JOptionPane.showMessageDialog(
                this, getResourceConverter().getString(
                    "workbench.new.MDXQuery.no.selection",
                    "No Mondrian connection. Select a Schema to connect."),
                getResourceConverter().getString(
                    "workbench.new.MDXQuery.no.selection.title", "Alert"),
                JOptionPane.WARNING_MESSAGE);
        }
    }

    // inform all opened mdx query windows about the list of opened schema files
    private void updateMDXCatalogList() {
        Iterator<JInternalFrame> it = mdxWindows.iterator();
        while (it.hasNext()) {
            JInternalFrame elem = it.next();
            QueryPanel qp = (QueryPanel) elem.getContentPane().getComponent(0);
            qp.setSchemaWindowMap(schemaWindowMap);
        }
    }

    /**
     * returns the currently selected schema explorer object
     *
     * @return current schema explorer object
     */
    public SchemaExplorer getCurrentSchemaExplorer() {
        JInternalFrame jf = desktopPane.getSelectedFrame();
        if (jf != null && jf.getContentPane().getComponentCount() > 0 && jf
            .getContentPane().getComponent(0) instanceof SchemaExplorer)
        {
            return (SchemaExplorer) jf.getContentPane().getComponent(0);
        }
        return null;
    }

    private void saveAsMenuItemActionPerformed(ActionEvent evt) {
        JInternalFrame jf = desktopPane.getSelectedFrame();

        if (jf != null && jf.getContentPane()
            .getComponent(0) instanceof SchemaExplorer)
        {
            SchemaExplorer se =
                (SchemaExplorer) jf.getContentPane().getComponent(0);
            java.io.File schemaFile = se.getSchemaFile();
            java.io.File oldSchemaFile = schemaFile;
            java.io.File suggSchemaFile = new File(
                schemaFile == null
                    ? se.getSchema().name.trim() + ".xml"
                    : schemaFile.getName());
            MondrianGuiDef.Schema schema = se.getSchema();
            JFileChooser jfc = new JFileChooser();
            MondrianProperties.instance();

            jfc.setSelectedFile(suggSchemaFile);

            if (!isSchemaValid(schema)) {
                // the schema would not be re-loadable.  Abort save.
                return;
            }

            if (jfc.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
                try {
                    schemaFile = jfc.getSelectedFile();
                    if (!oldSchemaFile.equals(schemaFile) && schemaFile
                        .exists())
                    {  // new file already exists, check for overwrite
                        int answer = JOptionPane.showConfirmDialog(
                            null,
                            getResourceConverter().getFormattedString(
                                "workbench.saveAs.schema.confirm",
                                "{0} schema file already exists. Do you want to replace it?",
                                schemaFile.getAbsolutePath()),
                            getResourceConverter().getString(
                                "workbench.saveAs.schema.confirm.title",
                                "Save As"),
                            JOptionPane.YES_NO_OPTION);
                        if (answer == 1) { //  no=1 ; yes=0
                            return;
                        }
                    }

                    if (se.isNewFile() && !oldSchemaFile.equals(schemaFile)) {
                        oldSchemaFile.delete();
                    }

                    if (se.isNewFile()) {
                        se.setNewFile(false);
                    }
                    se.setDirty(false);
                    se.setDirtyFlag(false);

                    XMLOutput out =
                        new XMLOutput(
                            new java.io.FileWriter(jfc.getSelectedFile()));
                    out.setAlwaysQuoteCData(true);
                    out.setIndentString("  ");
                    schema.displayXML(out);
                    se.setSchemaFile(schemaFile);
                    se.setTitle();  // sets title of iframe
                    setLastUsed(
                        jfc.getSelectedFile().getName(),
                        jfc.getSelectedFile().toURI().toURL().toString());

                    // Update menu item with new file name, then update catalog
                    // list for mdx queries
                    JMenuItem sMenuItem = schemaWindowMap.get(jf);
                    String mtexttokens[] = sMenuItem.getText().split(" ");
                    sMenuItem.setText(
                        mtexttokens[0] + " " + se.getSchemaFile().getName());
                    // Schema menu item updated, now update mdx query windows
                    // with updated catalog list.
                    updateMDXCatalogList();
                } catch (Exception ex) {
                    LOGGER.error(ex);
                }
            }
        }
    }

    /**
     * Validates that the schema can be parsed and loaded,
     * showing a warning message if any errors are encountered.
     */
    private boolean isSchemaValid(MondrianGuiDef.Schema schema) {
        try {
            StringWriter writer = new StringWriter();
            XMLOutput xmlOutput =  new XMLOutput(writer);
            schema.displayXML(xmlOutput);
            Parser xmlParser = XOMUtil.createDefaultParser();
            Reader reader = new StringReader(writer.getBuffer().toString());
            // attempt to create a new schema
            new MondrianGuiDef.Schema(xmlParser.parse(reader));
        } catch (XOMException e) {
            JOptionPane.showMessageDialog(
                this,
                getResourceConverter().getFormattedString(
                    "workbench.save.invalid.schema",
                    "Please correct the following error before saving:",
                    e.getLocalizedMessage()),
                getResourceConverter().getFormattedString(
                    "workbench.save.invalid.schema.title",
                    "Cannot Save"),
                JOptionPane.WARNING_MESSAGE);
            return false;
        }
        return true;
    }

    private void viewXMLMenuItemActionPerformed(ActionEvent evt) {
        JInternalFrame jf = desktopPane.getSelectedFrame();
        boolean oldValue = viewXmlMenuItem.getState();
        if (jf != null
            && jf.getContentPane().getComponent(0) instanceof SchemaExplorer)
        {
            SchemaExplorer se =
                (SchemaExplorer) jf.getContentPane().getComponent(0);
            // Call schema explorer's view xml event and update the workbench's
            // view menu accordingly'
            ((JCheckBoxMenuItem) evt.getSource()).setSelected(se.editMode(evt));
            return;
        }
        viewXmlMenuItem.setSelected(!oldValue);
    }

    public void saveMenuItemActionPerformed(ActionEvent evt) {
        JInternalFrame jf = desktopPane.getSelectedFrame();

        // Don't save if nothing there
        if (jf == null || jf.getContentPane() == null) {
            return;
        }

        if (jf.getContentPane().getComponent(0) instanceof SchemaExplorer) {
            SchemaExplorer se =
                (SchemaExplorer) jf.getContentPane().getComponent(0);

            java.io.File schemaFile = se.getSchemaFile();

            if (se.isNewFile()) {
                saveAsMenuItemActionPerformed(evt);
                return;
            }

            se.setDirty(false);
            se.setDirtyFlag(false);
            se.setTitle();  // sets title of iframe

            MondrianGuiDef.Schema schema = se.getSchema();

            if (!isSchemaValid(schema)) {
                // the schema would not be re-loadable.  Abort save.
                return;
            }

            MondrianProperties.instance();
            try {
                XMLOutput out = new XMLOutput(new FileWriter(schemaFile));
                out.setAlwaysQuoteCData(true);
                out.setIndentString("  ");
                schema.displayXML(out);
                setLastUsed(
                    schemaFile.getName(),
                    schemaFile.toURI().toURL().toString());
            } catch (Exception ex) {
                LOGGER.error("saveMenuItemActionPerformed", ex);
            }
        }
    }

    /**
     * Set last used in properties file
     */
    private void setLastUsed(String name, String url) {
        int match = 4;
        String luName = null;
        String propname = null;
        String lastUsed = "lastUsed";
        String lastUsedUrl = "lastUsedUrl";
        for (int i = 1; i <= 4; i++) {
            propname = lastUsed + i;
            luName = getWorkbenchProperty(propname);

            if (luName != null && luName.equals(name)) {
                match = i;
                break;
            }
        }

        for (int i = match; i > 1; i--) {
            if (getWorkbenchProperty(lastUsed + (i - 1)) != null) {
                setWorkbenchProperty(
                    lastUsed + i, getWorkbenchProperty(
                        lastUsed + (i - 1)));
                setWorkbenchProperty(
                    lastUsedUrl + i, getWorkbenchProperty(
                        lastUsedUrl + (i - 1)));
            }
        }

        setWorkbenchProperty(LAST_USED1, name);
        setWorkbenchProperty(LAST_USED1_URL, url);
        updateLastUsedMenu();
        storeWorkbenchProperties();
        storeDatabaseMeta();
    }

    private void updateLastUsedMenu() {
        if (getWorkbenchProperty(LAST_USED1) == null) {
            jSeparator2.setVisible(false);
        } else {
            jSeparator2.setVisible(true);
        }

        if (getWorkbenchProperty(LAST_USED1) != null) {
            lastUsed1MenuItem.setVisible(true);
        } else {
            lastUsed1MenuItem.setVisible(false);
        }
        if (getWorkbenchProperty(LAST_USED2) != null) {
            lastUsed2MenuItem.setVisible(true);
        } else {
            lastUsed2MenuItem.setVisible(false);
        }
        if (getWorkbenchProperty(LAST_USED3) != null) {
            lastUsed3MenuItem.setVisible(true);
        } else {
            lastUsed3MenuItem.setVisible(false);
        }
        if (getWorkbenchProperty(LAST_USED4) != null) {
            lastUsed4MenuItem.setVisible(true);
        } else {
            lastUsed4MenuItem.setVisible(false);
        }

        lastUsed1MenuItem.setText(getWorkbenchProperty(LAST_USED1));
        lastUsed2MenuItem.setText(getWorkbenchProperty(LAST_USED2));
        lastUsed3MenuItem.setText(getWorkbenchProperty(LAST_USED3));
        lastUsed4MenuItem.setText(getWorkbenchProperty(LAST_USED4));
    }

    private void lastUsed1MenuItemActionPerformed(ActionEvent evt) {
        try {
            openSchemaFrame(
                new File(new URI(getWorkbenchProperty(LAST_USED1_URL))), false);
        } catch (Exception e) {
            // probably URISyntaxException
            LOGGER.error("lastUsed1MenuItemActionPerformed", e);
        }
    }

    private void lastUsed2MenuItemActionPerformed(ActionEvent evt) {
        try {
            openSchemaFrame(
                new File(new URI(getWorkbenchProperty(LAST_USED2_URL))), false);
            setLastUsed(
                getWorkbenchProperty(LAST_USED2), getWorkbenchProperty(
                    LAST_USED2_URL));
        } catch (URISyntaxException e) {
            LOGGER.error("lastUsed2MenuItemActionPerformed", e);
        }
    }

    private void lastUsed3MenuItemActionPerformed(ActionEvent evt) {
        try {
            openSchemaFrame(
                new File(new URI(getWorkbenchProperty(LAST_USED3_URL))), false);
            setLastUsed(
                getWorkbenchProperty(LAST_USED3), getWorkbenchProperty(
                    LAST_USED3_URL));
        } catch (URISyntaxException e) {
            LOGGER.error("lastUsed3MenuItemActionPerformed", e);
        }
    }

    private void lastUsed4MenuItemActionPerformed(ActionEvent evt) {
        try {
            openSchemaFrame(
                new File(new URI(getWorkbenchProperty(LAST_USED4_URL))), false);
            setLastUsed(
                getWorkbenchProperty(LAST_USED4), getWorkbenchProperty(
                    LAST_USED4_URL));
        } catch (URISyntaxException e) {
            LOGGER.error("lastUsed4MenuItemActionPerformed", e);
        }
    }

    private void openSchemaFrame(File file, boolean newFile) {
        try {
            setCursor(Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR));

            if (!newFile) {
                // check if file not already open
                if (checkFileOpen(file)) {
                    return;
                }
                // check if schema file exists
                if (!file.exists()) {
                    JOptionPane.showMessageDialog(
                        this,
                        getResourceConverter().getFormattedString(
                            "workbench.open.schema.not.found",
                            "{0} File not found.",
                            file.getAbsolutePath()),
                        getResourceConverter().getString(
                            "workbench.open.schema.not.found.title", "Alert"),
                        JOptionPane.WARNING_MESSAGE);
                    return;
                }
                // check if file is writable
                if (!file.canWrite()) {
                    JOptionPane.showMessageDialog(
                        this,
                        getResourceConverter().getFormattedString(
                            "workbench.open.schema.not.writeable",
                            "{0} is not writeable.",
                            file.getAbsolutePath()),
                        getResourceConverter().getString(
                            "workbench.open.schema.not.writeable.title",
                            "Alert"),
                        JOptionPane.WARNING_MESSAGE);
                    return;
                }
                checkSchemaFile(file);
            }

            final JInternalFrame schemaFrame = new JInternalFrame();
            schemaFrame.setTitle(
                getResourceConverter().getFormattedString(
                    "workbench.open.schema.title",
                    "Schema - {0}",
                    file.getName()));

            getNewJdbcMetadata();

            schemaFrame.getContentPane().add(
                new SchemaExplorer(
                    this, file, jdbcMetaData, newFile, schemaFrame));

            String errorOpening =
                ((SchemaExplorer) schemaFrame.getContentPane().getComponent(0))
                    .getErrMsg();
            if (errorOpening != null) {
                JOptionPane.showMessageDialog(
                    this,
                    getResourceConverter().getFormattedString(
                        "workbench.open.schema.error",
                        "Error opening schema - {0}.",
                        errorOpening),
                    getResourceConverter().getString(
                        "workbench.open.schema.error.title", "Error"),
                    JOptionPane.ERROR_MESSAGE);
                schemaFrame.setClosed(true);
                return;
            }

            schemaFrame.setBounds(0, 0, 1000, 650);
            schemaFrame.setClosable(true);
            schemaFrame.setIconifiable(true);
            schemaFrame.setMaximizable(true);
            schemaFrame.setResizable(true);
            schemaFrame.setVisible(true);

            desktopPane.add(
                schemaFrame, javax.swing.JLayeredPane.DEFAULT_LAYER);
            schemaFrame.show();
            schemaFrame.setMaximum(true);

            displayWarningOnFailedConnection();

            final javax.swing.JMenuItem schemaMenuItem =
                new javax.swing.JMenuItem();
            schemaMenuItem.setText(windowMenuMapIndex++ + " " + file.getName());
            schemaMenuItem.addActionListener(
                new ActionListener() {
                    public void actionPerformed(ActionEvent evt) {
                        try {
                            if (schemaFrame.isIcon()) {
                                schemaFrame.setIcon(false);
                            } else {
                                schemaFrame.setSelected(true);
                            }
                        } catch (Exception ex) {
                            LOGGER.error("schemaMenuItem", ex);
                        }
                    }
                });

            windowMenu.add(schemaMenuItem, 0);
            windowMenu.setEnabled(true);

            windowMenu.add(jSeparator3, -1);
            windowMenu.add(cascadeMenuItem, -1);
            windowMenu.add(tileMenuItem, -1);
            windowMenu.add(minimizeMenuItem, -1);
            windowMenu.add(maximizeMenuItem, -1);
            windowMenu.add(closeAllMenuItem, -1);

            // add the file details in menu map
            schemaWindowMap.put(schemaFrame, schemaMenuItem);
            updateMDXCatalogList();

            schemaFrame.setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);

            schemaFrame.addInternalFrameListener(
                new InternalFrameAdapter() {
                    public void internalFrameClosing(InternalFrameEvent e) {
                        if (schemaFrame.getContentPane()
                            .getComponent(0) instanceof SchemaExplorer)
                        {
                            SchemaExplorer se =
                                (SchemaExplorer) schemaFrame.getContentPane()
                                    .getComponent(0);
                            int response = confirmFrameClose(schemaFrame, se);
                            if (response == 3) {    // not dirty
                                if (se.isNewFile()) {
                                    se.getSchemaFile().delete();
                                }
                                // default case for no save and not dirty
                                schemaWindowMap.remove(schemaFrame);
                                updateMDXCatalogList();
                                schemaFrame.dispose();
                                windowMenu.remove(schemaMenuItem);
                            }
                        }
                    }
                });

            schemaFrame.setFocusable(true);
            schemaFrame.addFocusListener(
                new FocusAdapter() {
                    public void focusGained(FocusEvent e) {
                        if (schemaFrame.getContentPane()
                            .getComponent(0) instanceof SchemaExplorer)
                        {
                            SchemaExplorer se = (SchemaExplorer)
                                schemaFrame.getContentPane().getComponent(0);
                            // update view menu based on schemaframe who gained
                            // focus
                            viewXmlMenuItem.setSelected(
                                se.isEditModeXML());
                        }
                    }

                    public void focusLost(FocusEvent e) {
                        if (schemaFrame.getContentPane()
                            .getComponent(0) instanceof SchemaExplorer)
                        {
                            SchemaExplorer se = (SchemaExplorer)
                                schemaFrame.getContentPane().getComponent(0);
                            // update view menu based on
                            viewXmlMenuItem.setSelected(
                                se.isEditModeXML());
                        }
                    }
                });
            viewXmlMenuItem.setSelected(false);
        } catch (Exception ex) {
            LOGGER.error("openSchemaFrame", ex);
        } finally {
            setCursor(Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR));
        }
    }

    private void openMenuItemActionPerformed(ActionEvent evt) {
        JFileChooser jfc = new JFileChooser();
        try {
            jfc.setFileSelectionMode(JFileChooser.FILES_ONLY);
            jfc.setFileFilter(
                new javax.swing.filechooser.FileFilter() {
                    public boolean accept(File pathname) {
                        return pathname.getName().toLowerCase().endsWith(".xml")
                               || pathname.isDirectory();
                    }

                    public String getDescription() {
                        return getResourceConverter().getString(
                            "workbench.open.schema.file.type",
                            "Mondrian Schema files (*.xml)");
                    }
                });

            String lastUsed = getWorkbenchProperty(LAST_USED1_URL);

            if (lastUsed != null) {
                jfc.setCurrentDirectory(new File(new URI(lastUsed)));
            }
        } catch (Exception ex) {
            LOGGER.error("Could not set file chooser", ex);
        }
        MondrianProperties.instance();
        if (jfc.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
            try {
                setLastUsed(
                    jfc.getSelectedFile().getName(),
                    jfc.getSelectedFile().toURI().toURL().toString());
            } catch (MalformedURLException e) {
                LOGGER.error(e);
            }

            openSchemaFrame(jfc.getSelectedFile(), false);
        }
    }

    // checks if file already open in schema explorer
    private boolean checkFileOpen(File file) {
        Iterator<JInternalFrame> it = schemaWindowMap.keySet().iterator();
        // keys=schemaframes
        while (it.hasNext()) {
            JInternalFrame elem = it.next();
            File f = ((SchemaExplorer) elem.getContentPane().getComponent(0))
                .getSchemaFile();
            if (f.equals(file)) {
                try {
                    // make the schema file active
                    elem.setSelected(true);
                    return true;
                } catch (Exception ex) {
                    // remove file from map as schema frame does not exist
                    schemaWindowMap.remove(elem);
                    break;
                }
            }
        }
        return false;
    }

    private void getNewJdbcMetadata() {
        jdbcMetaData = new JdbcMetaData(
            this,
            jdbcDriverClassName,
            jdbcConnectionUrl,
            jdbcUsername,
            jdbcPassword,
            jdbcSchema,
            requireSchema);
    }

    /**
     * Updates the JdbcMetaData for each SchemaExplorer contained in each Schema
     * Frame currently opened based on the JDBC preferences entered.
     */
    private void resetWorkbench() {
        getNewJdbcMetadata();

        Iterator<JInternalFrame> theSchemaFrames =
            schemaWindowMap.keySet().iterator();
        while (theSchemaFrames.hasNext()) {
            JInternalFrame theSchemaFrame =
                theSchemaFrames.next();
            SchemaExplorer theSchemaExplorer =
                (SchemaExplorer) theSchemaFrame.getContentPane()
                    .getComponent(0);
            File theFile = theSchemaExplorer.getSchemaFile();
            checkSchemaFile(theFile);
            theSchemaExplorer.resetMetaData(jdbcMetaData);
            theSchemaExplorer.getTreeUpdater().update();
            theSchemaFrame.updateUI();
        }
        // EC: If the JDBC preferences entered then display a warning.
        displayWarningOnFailedConnection();

        for (JInternalFrame jdbcFrame : jdbcWindows) {
            JdbcExplorer explorer =
                (JdbcExplorer) jdbcFrame.getContentPane().getComponent(0);
            explorer.resetMetaData(jdbcMetaData);

            jdbcFrame.setTitle(
                getResourceConverter().getFormattedString(
                    "workbench.new.JDBCExplorer.title",
                    "JDBC Explorer - {0} {1}",
                    jdbcMetaData.getDatabaseProductName(),
                    jdbcMetaData.getJdbcConnectionUrl()));

            explorer.getTreeUpdater().update();
            explorer.updateUI();
        }
    }

    /**
     * Display jdbc connection status warning, if connection is uncsuccessful.
     */
    private void displayWarningOnFailedConnection() {
        if (jdbcMetaData != null && jdbcMetaData.getErrMsg() != null) {
            JOptionPane.showMessageDialog(
                this,
                getResourceConverter().getFormattedString(
                    "workbench.open.schema.jdbc.error",
                    "Database connection could not be done.\n{0}\nAll validations related to database will be ignored.",
                    jdbcMetaData.getErrMsg()),
                getResourceConverter().getString(
                    "workbench.open.schema.jdbc.error.title", "Alert"),
                JOptionPane.WARNING_MESSAGE);
        }
    }

    /**
     * Check if schema file is valid by initiating a mondrian connection.
     */
    private void checkSchemaFile(File file) {
        try {
            // this connection parses the catalog file which if invalid will
            // throw exception
            PropertyList list = new PropertyList();
            list.put("Provider", "mondrian");
            list.put("Jdbc", jdbcConnectionUrl);
            list.put("Catalog", file.toURI().toURL().toString());
            list.put("JdbcDrivers", jdbcDriverClassName);
            if (jdbcUsername != null && jdbcUsername.length() > 0) {
                list.put("JdbcUser", jdbcUsername);
            }
            if (jdbcPassword != null && jdbcPassword.length() > 0) {
                list.put("JdbcPassword", jdbcPassword);
            }

            DriverManager.getConnection(list, null);
        } catch (Exception ex) {
            LOGGER.error(
                "Exception : Schema file "
                + file.getAbsolutePath()
                + " is invalid."
                + ex.getMessage(), ex);
        } catch (Error err) {
            LOGGER.error(
                "Error : Schema file "
                + file.getAbsolutePath()
                + " is invalid."
                + err.getMessage(), err);
        }
    }


    private void exitMenuItemActionPerformed(ActionEvent evt) {
        storeWorkbenchProperties();
        storeDatabaseMeta();
        closeAllSchemaFrames(true);
    }

    /**
     * Parses arguments passed into Workbench.
     *
     * <p>Right now, it's very simple.  Just search through the list of
     * arguments.  If it begins with -f=, then the rest is a file name.  Ignore
     * any others.  We can make this more complicated later if we need to.
     *
     * @param args the command line arguments
     */
    private void parseArgs(String args[]) {
        for (int argNum = 0; argNum < args.length; argNum++) {
            if (args[argNum].startsWith("-f=")) {
                openFile = args[argNum].substring(3);
            }
        }
    }

    public String getTooltip(String titleName) {
        try {
            return getWorkbenchResourceBundle().getString(titleName);
        } catch (MissingResourceException e) {
            return getResourceConverter().getFormattedString(
                "workbench.tooltip.error",
                "No help available for {0}",
                titleName);
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        Workbench w = null;
        try {
            w = new Workbench();
            w.parseArgs(args);
            w.setSize(800, 600);
            // if user specified a file to open, do so now.
            if (w.openFile != null) {
                File f = new File(w.openFile);
                if (f.canRead()) {
                    w.openSchemaFrame(
                        f.getAbsoluteFile(),
                        // parameter to indicate this is a new or existing
                        // catalog file
                        false);
                }
            }
            w.setVisible(true);
        } catch (Throwable ex) {
            if (w != null) {
            JOptionPane.showMessageDialog(
                w,
                w.getResourceConverter().getFormattedString(
                    "workbench.main.uncoverable_error",
                    "Pentaho Schema Workbench has encountered an unrecoverable error. \n{0}",
                    ex.getLocalizedMessage()),
                w.getResourceConverter().getString(
                    "workbench.main.uncoverable_error.title",
                    "PSW Fatal Error"),
                JOptionPane.ERROR_MESSAGE);
            }
            LOGGER.error("main", ex);
        }
    }

    // Variables declaration - do not modify
    private javax.swing.JButton toolbarSaveAsButton;
    private javax.swing.JMenuItem openMenuItem;
    private javax.swing.JMenuItem lastUsed1MenuItem;
    private javax.swing.JMenuItem lastUsed2MenuItem;
    private javax.swing.JMenuItem lastUsed3MenuItem;
    private javax.swing.JMenuItem lastUsed4MenuItem;
    private javax.swing.JMenu fileMenu;
    private javax.swing.JMenuItem newQueryMenuItem;
    private javax.swing.JMenuItem newQueryMenuItem2;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JPanel jPanel2;
    private javax.swing.JButton toolbarOpenButton;
    private javax.swing.JButton toolbarNewButton;
    private javax.swing.JButton toolbarNewArrowButton;
    private javax.swing.JSeparator jSeparator1;
    private javax.swing.JSeparator jSeparator2;
    private javax.swing.JSeparator jSeparator3;
    private javax.swing.JMenuItem cutMenuItem;
    private javax.swing.JMenuBar menuBar;
    private javax.swing.JMenuItem saveMenuItem;
    private javax.swing.JMenuItem newJDBCExplorerMenuItem;
    private javax.swing.JMenuItem newJDBCExplorerMenuItem2;
    private javax.swing.JButton toolbarSaveButton;
    private javax.swing.JMenuItem copyMenuItem;
    private javax.swing.JDesktopPane desktopPane;
    private javax.swing.JMenu viewMenu;
    private javax.swing.JMenu toolsMenu;
    private javax.swing.JMenu newMenu;
    private javax.swing.JMenuItem deleteMenuItem;
    private javax.swing.JMenuItem newSchemaMenuItem;
    private javax.swing.JMenuItem newSchemaMenuItem2;
    private javax.swing.JMenuItem exitMenuItem;
    private javax.swing.JButton toolbarPreferencesButton;
    private javax.swing.JCheckBoxMenuItem requireSchemaCheckboxMenuItem;
    private javax.swing.JMenu editMenu;
    private javax.swing.JMenuItem pasteMenuItem;
    private javax.swing.JMenuItem preferencesMenuItem;
    private javax.swing.JCheckBoxMenuItem viewXmlMenuItem;
    private javax.swing.JMenuItem saveAsMenuItem;
    private javax.swing.JToolBar jToolBar1;
    private javax.swing.JToolBar jToolBar2;
    private javax.swing.JPopupMenu toolbarNewPopupMenu;
    private javax.swing.JMenu windowMenu;
    private javax.swing.JMenu helpMenu;
    private javax.swing.JMenuItem aboutMenuItem;
    private javax.swing.JMenuItem cascadeMenuItem;
    private javax.swing.JMenuItem tileMenuItem;
    private javax.swing.JMenuItem minimizeMenuItem;
    private javax.swing.JMenuItem maximizeMenuItem;
    private javax.swing.JMenuItem closeAllMenuItem;
// End of variables declaration

    /**
     * Used by schema framewhen it uses 'view xml' to update view xml menu item
     */
    public javax.swing.JCheckBoxMenuItem getViewXmlMenuItem() {
        return viewXmlMenuItem;
    }
}

// End Workbench.java
