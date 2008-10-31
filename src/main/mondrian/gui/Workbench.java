/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 1999-2002 Kana Software, Inc.
// Copyright (C) 2001-2008 Julian Hyde and others
// Copyright (C) 2006-2007 Cincom Systems, Inc.
// Copyright (C) 2006-2007 JasperSoft
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// Created on September 26, 2002, 11:28 AM
// Modified on 15-Jun-2003 by ebengtso
//
 */

package mondrian.gui;


import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.util.Iterator;
import java.util.Map;
import javax.swing.event.InternalFrameAdapter;
import javax.swing.filechooser.FileSystemView;
import mondrian.olap.DriverManager;
import mondrian.olap.MondrianProperties;
import mondrian.olap.Util.PropertyList;
import mondrian.rolap.agg.AggregationManager;

import javax.swing.*;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.MissingResourceException;
import java.util.Vector;
import javax.swing.event.InternalFrameEvent;
import javax.swing.plaf.basic.BasicArrowButton;
import javax.swing.text.DefaultEditorKit;

import org.apache.log4j.Logger;
import org.eigenbase.xom.XMLOutput;

/**
 *
 * @author  sean
 * @version $Id$
 */
public class Workbench extends javax.swing.JFrame {

    static String WORKBENCH_USER_HOME_DIR;
    static String WORKBENCH_CONFIG_FILE;

    private static final String LAST_USED1 = "lastUsed1";
    private static final String LAST_USED1_URL = "lastUsedUrl1";
    private static final String LAST_USED2 = "lastUsed2";
    private static final String LAST_USED2_URL = "lastUsedUrl2";
    private static final String LAST_USED3 = "lastUsed3";
    private static final String LAST_USED3_URL = "lastUsedUrl3";
    private static final String LAST_USED4 = "lastUsed4";
    private static final String LAST_USED4_URL = "lastUsedUrl4";
    private static final String WorkbenchInfoResourceName = "mondrian.gui.resources.workbenchInfo";
    private static final String GUIResourceName = "mondrian.gui.resources.gui";
    private static final String TextResourceName = "mondrian.gui.resources.text";

    private static final Logger LOGGER = Logger.getLogger(Workbench.class);

    private String jdbcDriverClassName;
    private String jdbcConnectionUrl;
    private String jdbcUsername;
    private String jdbcPassword;
    private String jdbcSchema;
    private boolean requireSchema;

    private JDBCMetaData jdbcMetaData;

    private final ClassLoader myClassLoader;

    private final ResourceBundle guiResourceBundle;
    private final ResourceBundle textResourceBundle;

    private Properties workbenchProperties;
    private static ResourceBundle workbenchResourceBundle = null;

    private I18n resourceConverter = null;

    private static int newSchema = 1;

    private String openFile = null;

    private Map schemaWindowMap = new HashMap();    // map of schema frames and its menu items (JInternalFrame -> JMenuItem)
    private Vector mdxWindows = new Vector();
    private int windowMenuMapIndex = 1;

    /** Creates new form Workbench */
    public Workbench() {
        myClassLoader = this.getClass().getClassLoader();

        guiResourceBundle = ResourceBundle.getBundle(GUIResourceName, Locale.getDefault(), myClassLoader);
        textResourceBundle = ResourceBundle.getBundle(TextResourceName, Locale.getDefault(), myClassLoader);

        resourceConverter = new I18n(guiResourceBundle, textResourceBundle);

        // Setting User home directory
        WORKBENCH_USER_HOME_DIR = System.getProperty("user.home") + File.separator + ".schemaWorkbench";
        WORKBENCH_CONFIG_FILE = WORKBENCH_USER_HOME_DIR + File.separator + "workbench.properties";

        loadWorkbenchProperties();
        initDataSource();
        initComponents();
        loadMenubarPlugins();

        ImageIcon icon = new javax.swing.ImageIcon(myClassLoader.getResource(resourceConverter.getGUIReference("cube")));

        this.setIconImage(icon.getImage());
    }

    /**
     * load properties
     */
    private void loadWorkbenchProperties() {
        workbenchProperties = new Properties();
        try {
            workbenchResourceBundle = ResourceBundle.getBundle(WorkbenchInfoResourceName, Locale.getDefault(), myClassLoader);

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
     * @param key property key
     * @param value property value
     */
    public void setWorkbenchProperty(String key, String value) {
        workbenchProperties.setProperty(key, value);
    }

    /**
     * save properties
     */
    public void storeWorkbenchProperties() {
        //save properties to file
        File dir = new File(WORKBENCH_USER_HOME_DIR);
        try {
            if (dir.exists()) {
                if (!dir.isDirectory()) {
                    JOptionPane.showMessageDialog(this,
                            getResourceConverter().getFormattedString("workbench.user.home.not.directory",
                                        "{0} is not a directory!\nPlease rename this file and retry to save configuration!", new String[] {WORKBENCH_USER_HOME_DIR}),
                                        "", JOptionPane.ERROR_MESSAGE);
                    return;
                }
            } else {
                dir.mkdirs();
            }
        } catch (Exception ex) {
            LOGGER.error("storeWorkbenchProperties: mkdirs", ex);
            JOptionPane.showMessageDialog(this,
                    getResourceConverter().getFormattedString("workbench.user.home.exception",
                            "An error is occurred creating workbench configuration directory:\n{0}\nError is: {1}",
                                new String[] {WORKBENCH_USER_HOME_DIR, ex.getLocalizedMessage()}),
                            "", JOptionPane.ERROR_MESSAGE);
            return;
        }

        OutputStream out = null;
        try {
            out = (OutputStream) new FileOutputStream(new File(WORKBENCH_CONFIG_FILE));
            workbenchProperties.store(out, "Workbench configuration");
        } catch (Exception e) {
            LOGGER.error("storeWorkbenchProperties: store", e);
            JOptionPane.showMessageDialog(this,
                    getResourceConverter().getFormattedString("workbench.save.configuration",
                            "An error is occurred creating workbench configuration file:\n{0}\nError is: {1}",
                            new String[] {WORKBENCH_CONFIG_FILE, e.getLocalizedMessage()}),
                            "", JOptionPane.ERROR_MESSAGE);
        } finally {
            try {
                out.close();
            } catch (IOException eIO) {
                LOGGER.error("storeWorkbenchProperties: out.close", eIO);
            }
        }
    }

    /**
     * Initialize the data source from a property file
     */
    private void initDataSource() {
        jdbcDriverClassName = workbenchProperties.getProperty("jdbcDriverClassName");
        jdbcConnectionUrl = workbenchProperties.getProperty("jdbcConnectionUrl");
        jdbcUsername = workbenchProperties.getProperty("jdbcUsername");
        jdbcPassword = workbenchProperties.getProperty("jdbcPassword");
        jdbcSchema = workbenchProperties.getProperty("jdbcSchema");
        requireSchema = "true".equals(workbenchProperties.getProperty("requireSchema"));
    }

    /** This method is called from within the constructor to
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
        jSeparator4 = new javax.swing.JSeparator();
        exitMenuItem = new javax.swing.JMenuItem();
        windowMenu = new javax.swing.JMenu();
        helpMenu = new javax.swing.JMenu();
        editMenu = new javax.swing.JMenu();
        cutMenuItem = new javax.swing.JMenuItem(new DefaultEditorKit.CutAction());
        copyMenuItem = new javax.swing.JMenuItem(new DefaultEditorKit.CopyAction());
        pasteMenuItem = new javax.swing.JMenuItem(new DefaultEditorKit.PasteAction());
        deleteMenuItem = new javax.swing.JMenuItem();
        aboutMenuItem = new javax.swing.JMenuItem();
        toolsMenu = new javax.swing.JMenu();
        viewMenu = new javax.swing.JMenu();
        viewDimensionsMenuItem = new javax.swing.JCheckBoxMenuItem();
        viewMeasuresMenuItem = new javax.swing.JCheckBoxMenuItem();
        viewCubesMenuItem = new javax.swing.JCheckBoxMenuItem();
        viewXMLMenuItem = new javax.swing.JCheckBoxMenuItem();

        setTitle(getResourceConverter().getString("workbench.panel.title","Schema Workbench"));
        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new java.awt.event.WindowAdapter() {
            public void windowClosing(java.awt.event.WindowEvent evt) {
                storeWorkbenchProperties();
                closeAllSchemaFrames(true);
            }
        });

        getContentPane().add(desktopPane, java.awt.BorderLayout.CENTER);


        newSchemaMenuItem2.setText(getResourceConverter().getString("workbench.menu.newSchema","Schema"));
        //newSchemaMenuItem2.setText("Schema");
        newSchemaMenuItem2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                newSchemaMenuItemActionPerformed(evt);
            }
        });


        newQueryMenuItem2.setText(getResourceConverter().getString("workbench.menu.newQuery","MDX Query"));
        //newQueryMenuItem2.setText("MDX Query");
        newQueryMenuItem2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                newQueryMenuItemActionPerformed(evt);
            }
        });


        newJDBCExplorerMenuItem2.setText(getResourceConverter().getString("workbench.menu.newJDBC","JDBC Explorer"));
        //newJDBCExplorerMenuItem2.setText("JDBC Explorer");
        newJDBCExplorerMenuItem2.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                newJDBCExplorerMenuItemActionPerformed(evt);
            }
        });


        toolbarNewPopupMenu.add(newSchemaMenuItem2);
        toolbarNewPopupMenu.add(newQueryMenuItem2);
        toolbarNewPopupMenu.add(newJDBCExplorerMenuItem2);


        jPanel2.setLayout(new java.awt.BorderLayout());
        jPanel2.setBorder(javax.swing.BorderFactory.createEtchedBorder());
        jPanel2.setMaximumSize(new java.awt.Dimension(50, 28)); // old width=18

        toolbarNewButton.setIcon(new javax.swing.ImageIcon(getClass().getResource(resourceConverter.getGUIReference("new"))));
        toolbarNewButton.setToolTipText(getResourceConverter().getString("workbench.toolbar.new","New"));
        //toolbarNewButton.setToolTipText("New");
        toolbarNewButton.setBorderPainted(false);
        toolbarNewButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                toolbarNewPopupMenu.show(jPanel2,0,jPanel2.getSize().height);
            }
        });

        jToolBar2.setFloatable(false);
        jToolBar2.add(toolbarNewButton);

        jPanel2.add(jToolBar2,java.awt.BorderLayout.CENTER);

        toolbarNewArrowButton = new BasicArrowButton(SwingConstants.SOUTH);
        toolbarNewArrowButton.setToolTipText(getResourceConverter().getString("workbench.toolbar.newArrow","New"));
        //toolbarNewArrowButton.setToolTipText("New");
        toolbarNewArrowButton.setBorderPainted(false);
        toolbarNewArrowButton.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                toolbarNewPopupMenu.show(jPanel2,0,jPanel2.getSize().height);
            }
        });

        jPanel2.add(toolbarNewArrowButton,java.awt.BorderLayout.EAST);

        // toolbarNewButton.add(select); // error:none of the toolbar buttons are displayed
        // jToolBar1.add(select,1);    //error: arrow button is so wide it takes all remaining space on toolbar
        jToolBar1.add(jPanel2,0);




        toolbarOpenButton.setIcon(new javax.swing.ImageIcon(getClass().getResource(resourceConverter.getGUIReference("open"))));
        toolbarOpenButton.setToolTipText(getResourceConverter().getString("workbench.toolbar.open","Open"));
        //toolbarOpenButton.setToolTipText("Open");
        toolbarOpenButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                openMenuItemActionPerformed(evt);
            }
        });

        jToolBar1.add(toolbarOpenButton);


        toolbarSaveButton.setIcon(new javax.swing.ImageIcon(getClass().getResource(resourceConverter.getGUIReference("save"))));
        toolbarSaveButton.setToolTipText(getResourceConverter().getString("workbench.toolbar.save","Save"));
        toolbarSaveButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                saveMenuItemActionPerformed(evt);
            }
        });

        jToolBar1.add(toolbarSaveButton);

        toolbarSaveAsButton.setIcon(new javax.swing.ImageIcon(getClass().getResource(resourceConverter.getGUIReference("saveAs"))));
        toolbarSaveAsButton.setToolTipText(getResourceConverter().getString("workbench.toolbar.saveAs","Save As"));
        toolbarSaveAsButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                saveAsMenuItemActionPerformed(evt);
            }
        });

        jToolBar1.add(toolbarSaveAsButton);

        jPanel1.setMaximumSize(new java.awt.Dimension(8, 8)); //8, 32767
        jToolBar1.add(jPanel1);

        toolbarPreferencesButton.setIcon(new javax.swing.ImageIcon(getClass().getResource(resourceConverter.getGUIReference("preferences"))));
        toolbarPreferencesButton.setToolTipText(getResourceConverter().getString("workbench.toolbar.preferences","Preferences"));
        toolbarPreferencesButton.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                toolbarPreferencesButtonActionPerformed(evt);
            }
        });

        jToolBar1.add(toolbarPreferencesButton);


        getContentPane().add(jToolBar1, java.awt.BorderLayout.NORTH);

        fileMenu.setText(getResourceConverter().getString("workbench.menu.file","File"));
        newMenu.setText(getResourceConverter().getString("workbench.menu.new","New"));

        newSchemaMenuItem.setText(getResourceConverter().getString("workbench.menu.newSchema","Schema"));
        newSchemaMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                newSchemaMenuItemActionPerformed(evt);
            }
        });

        newMenu.add(newSchemaMenuItem);

        newQueryMenuItem.setText(getResourceConverter().getString("workbench.menu.newQuery","MDX Query"));
        newQueryMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                newQueryMenuItemActionPerformed(evt);
            }
        });

        newMenu.add(newQueryMenuItem);

        newJDBCExplorerMenuItem.setText(getResourceConverter().getString("workbench.menu.newJDBC","JDBC Explorer"));
        newJDBCExplorerMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                newJDBCExplorerMenuItemActionPerformed(evt);
            }
        });

        newMenu.add(newJDBCExplorerMenuItem);

        fileMenu.add(newMenu);

        openMenuItem.setText(getResourceConverter().getString("workbench.menu.open","Open"));
        openMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                openMenuItemActionPerformed(evt);
            }
        });

        fileMenu.add(openMenuItem);

        saveMenuItem.setText(getResourceConverter().getString("workbench.menu.save","Save"));
        saveMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                saveMenuItemActionPerformed(evt);
            }
        });

        fileMenu.add(saveMenuItem);

        saveAsMenuItem.setText(getResourceConverter().getString("workbench.menu.saveAsDot","Save As ..."));
        saveAsMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                saveAsMenuItemActionPerformed(evt);
            }
        });

        fileMenu.add(saveAsMenuItem);

        //add last used
        fileMenu.add(jSeparator2);

        lastUsed1MenuItem.setText(workbenchProperties.getProperty("lastUsed1"));
        lastUsed1MenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                lastUsed1MenuItemActionPerformed(evt);
            }
        });
        fileMenu.add(lastUsed1MenuItem);

        lastUsed2MenuItem.setText(workbenchProperties.getProperty("lastUsed2"));
        lastUsed2MenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                lastUsed2MenuItemActionPerformed(evt);
            }
        });
        fileMenu.add(lastUsed2MenuItem);

        lastUsed3MenuItem.setText(workbenchProperties.getProperty("lastUsed3"));
        lastUsed3MenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                lastUsed3MenuItemActionPerformed(evt);
            }
        });
        fileMenu.add(lastUsed3MenuItem);

        lastUsed4MenuItem.setText(workbenchProperties.getProperty("lastUsed4"));
        lastUsed4MenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                lastUsed4MenuItemActionPerformed(evt);
            }
        });
        fileMenu.add(lastUsed4MenuItem);

        updateLastUsedMenu();
        fileMenu.add(jSeparator1);

        exitMenuItem.setText(getResourceConverter().getString("workbench.menu.exit","Exit"));
        exitMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                exitMenuItemActionPerformed(evt);
            }
        });

        fileMenu.add(exitMenuItem);

        menuBar.add(fileMenu);

        editMenu.setText(getResourceConverter().getString("workbench.menu.edit","Edit"));
        cutMenuItem.setText(getResourceConverter().getString("workbench.menu.cut","Cut"));
        editMenu.add(cutMenuItem);

        copyMenuItem.setText(getResourceConverter().getString("workbench.menu.copy","Copy"));
        editMenu.add(copyMenuItem);

        pasteMenuItem.setText(getResourceConverter().getString("workbench.menu.paste","Paste"));
        editMenu.add(pasteMenuItem);

        deleteMenuItem.setText(getResourceConverter().getString("workbench.menu.delete","Delete"));
        editMenu.add(deleteMenuItem);

        menuBar.add(editMenu);

        viewMenu.setText(getResourceConverter().getString("workbench.menu.view","View"));
        viewXMLMenuItem.setText(getResourceConverter().getString("workbench.menu.viewXML","View XML"));
        viewXMLMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                viewXMLMenuItemActionPerformed(evt);
            }
        });
        viewMenu.add(viewXMLMenuItem);
        menuBar.add(viewMenu);

        toolsMenu.setText(getResourceConverter().getString("workbench.menu.tools","Tools"));
        preferencesMenuItem.setText(getResourceConverter().getString("workbench.menu.preferences","Preferences"));
        preferencesMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                toolbarPreferencesButtonActionPerformed(evt);
            }
        });
        toolsMenu.add(preferencesMenuItem);
        menuBar.add(toolsMenu);


        windowMenu.setText(getResourceConverter().getString("workbench.menu.windows","Windows"));

        cascadeMenuItem = new javax.swing.JMenuItem();
        cascadeMenuItem.setText(getResourceConverter().getString("workbench.menu.cascadeWindows","Cascade Windows"));
        cascadeMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                cascadeMenuItemActionPerformed(evt);
            }
        });

        tileMenuItem = new javax.swing.JMenuItem();
        tileMenuItem.setText(getResourceConverter().getString("workbench.menu.tileWindows","Tile Windows"));
        tileMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                tileMenuItemActionPerformed(evt);
            }
        });

        closeAllMenuItem = new javax.swing.JMenuItem();
        closeAllMenuItem.setText(getResourceConverter().getString("workbench.menu.closeAll","Close All"));
        closeAllMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                closeAllMenuItemActionPerformed(evt);
            }
        });

        minimizeMenuItem = new javax.swing.JMenuItem();
        minimizeMenuItem.setText(getResourceConverter().getString("workbench.menu.minimizeAll","Minimize All"));
        minimizeMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                minimizeMenuItemActionPerformed(evt);
            }
        });

        maximizeMenuItem = new javax.swing.JMenuItem();
        maximizeMenuItem.setText(getResourceConverter().getString("workbench.menu.maximizeAll","Maximize All"));
        maximizeMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                maximizeMenuItemActionPerformed(evt);
            }
        });

        menuBar.add(windowMenu);

        aboutMenuItem.setText(getResourceConverter().getString("workbench.menu.about","About"));
        aboutMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                aboutMenuItemActionPerformed(evt);
            }
        });

        helpMenu.add(aboutMenuItem);

        helpMenu.setText(getResourceConverter().getString("workbench.menu.help","Help"));
        menuBar.add(helpMenu);

        setJMenuBar(menuBar);

        pack();
    }

    /**
     * this method loads any available menubar plugins based on
     *
     */
    private void loadMenubarPlugins() {
        // render any plugins
        InputStream pluginStream = null;
        try {
            Properties props = new Properties();
            pluginStream =
                getClass().getResourceAsStream("/workbench_plugins.properties");
            if (pluginStream != null) {
                props.load(pluginStream);
                for (Object key : props.keySet()) {
                    String keystr = (String)key;
                    if (keystr.startsWith("workbench.menu-plugin")) {
                        String val = props.getProperty(keystr);
                        WorkbenchMenubarPlugin plugin =
                            (WorkbenchMenubarPlugin)Class.forName(val).newInstance();
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
     * @return the resources
     */
    public ResourceBundle getGUIResourceBundle() {
        return guiResourceBundle;
    }

    /**
     * @return the resourceConverter
     */
    public I18n getResourceConverter() {
        return resourceConverter;
    }

    private void tileMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        Dimension dsize = desktopPane.getSize();
        int desktopW = (int) dsize.getWidth();
        int desktopH = (int) dsize.getHeight();
        int darea =  (int) (desktopW * desktopH);

        double eacharea = darea / (schemaWindowMap.size() + mdxWindows.size());
        int wh = (int) Math.sqrt(eacharea);

        Iterator []its = new Iterator[2];

        its[0] = schemaWindowMap.keySet().iterator();   // keys = schemaframes
        its[1] = mdxWindows.iterator();

        JInternalFrame sf = null;
        int x = 0, y = 0;

        try {
            for (int i = 0; i < its.length; i++) {
                Iterator it = its[i];
                while (it.hasNext()) {
                    sf = (JInternalFrame) it.next();
                    if (sf != null) {
                        if (sf.isIcon()) {
                            //sf.setIcon(false);
                        } else {
                            sf.setMaximum(false);
                            sf.moveToFront();
                            if ((x >= desktopW) || (((desktopW - x) * wh) < (eacharea / 2))) {
                                // move to next row of windows
                                y += wh;
                                x = 0;
                            }
                            int sfwidth  = ((x + wh) < desktopW ? wh : desktopW - x);
                            int sfheight = ((y + wh) < desktopH ? wh : desktopH - y);
                            sf.setBounds(x, y, sfwidth, sfheight);
                            x += sfwidth;
                        }
                    }
                }
            }
        } catch (Exception ex) {
            LOGGER.error("tileMenuItemActionPerformed", ex);
            //do nothing
        }
    }
    // cascade all the indows open in schema workbench
    private void cascadeMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        Iterator []its = new Iterator[2];

        its[0] = schemaWindowMap.keySet().iterator();   // keys = schemaframes
        its[1] = mdxWindows.iterator();
        int sfi = 1;
        JInternalFrame sf = null;

        try {
            for (int i = 0; i < its.length; i++) {
                Iterator it = its[i];
                while (it.hasNext()) {
                    sf = (JInternalFrame) it.next();
                    if (sf != null) {
                        if (sf.isIcon()) {
                            //sf.setIcon(false);
                        } else {
                            sf.setMaximum(false);
                            sf.setLocation(30 * sfi, 30 * sfi);
                            sf.moveToFront();
                            sf.setSelected(true);
                            sfi++;
                        }
                    }
                }
            }
        } catch (Exception ex) {
            LOGGER.error("cascadeMenuItemActionPerformed", ex);
            //do nothing
        }
    }

    // close all the windows open in schema workbench
    private void closeAllMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        closeAllSchemaFrames(false);
    }

    private void closeAllSchemaFrames(boolean exitAfterClose) {
        Object [][] its = new Object[2][];  // initialize row dimension
        its[0] = schemaWindowMap.keySet().toArray();   // keys = schemaframes
        its[1] = mdxWindows.toArray();
        JInternalFrame sf = null;

        try {
            for (int i = 0; i < its.length; i++) {
                for (int j = 0; j < its[i].length; j++) {
                    sf = (JInternalFrame) its[i][j];
                    if (sf != null) {
                        if (sf.getContentPane().getComponent(0) instanceof SchemaExplorer) {
                            SchemaExplorer se = (SchemaExplorer) sf.getContentPane().getComponent(0);
                            sf.setSelected(true);
                            int response = confirmFrameClose(sf, se);
                            if (response == 2) {    // cancel
                                return;
                            }
                            if (response == 3) {    // not dirty
                                sf.setClosed(true);
                            }
                        }
                    }
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

    private int confirmFrameClose(JInternalFrame schemaFrame, SchemaExplorer se) {
        if (se.isDirty()) {
            JMenuItem schemaMenuItem = (JMenuItem) schemaWindowMap.get(desktopPane.getSelectedFrame());
            int answer =
                JOptionPane.showConfirmDialog(
                    null,
                    getResourceConverter().getFormattedString(
                        "workbench.saveSchemaOnClose.alert",
                        "Save changes to {0}?",
                        new String[] { se.getSchemaFile().toString() }),
                    getResourceConverter().getString("workbench.saveSchemaOnClose.title","Schema"),
                    JOptionPane.YES_NO_CANCEL_OPTION);
            switch (answer) { // yes=0; no=1; cancel=2
            case 0:
                saveMenuItemActionPerformed(null);
                schemaWindowMap.remove(schemaFrame); //schemaWindowMap.remove(se.getSchemaFile());
                updateMDXCatalogList();
                schemaFrame.dispose();
                windowMenu.remove(schemaMenuItem);  // follow this by removing file from schemaWindowMap
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

    private void minimizeMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        Iterator []its = new Iterator[2];

        its[0] = schemaWindowMap.keySet().iterator();   // values = schemaframes
        its[1] = mdxWindows.iterator();
        JInternalFrame sf;

        try {
            for (int i = 0; i < its.length; i++) {
                Iterator it = its[i];
                while (it.hasNext()) {
                    sf = (JInternalFrame) it.next();
                    if (sf != null) {
                        if (sf.isIcon()) {
                            //sf.setIcon(false);
                        } else {
                            sf.setIcon(true);
                        }
                    }
                }
            }
        } catch (Exception ex) {
            LOGGER.error("minimizeMenuItemActionPerformed", ex);
            //do nothing
        }
    }

    private void maximizeMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        Iterator []its = new Iterator[2];

        its[0] = schemaWindowMap.keySet().iterator();   // values = schemaframes
        its[1] = mdxWindows.iterator();
        JInternalFrame sf;

        try {
            for (int i = 0; i < its.length; i++) {
                Iterator it = its[i];
                while (it.hasNext()) {
                    sf = (JInternalFrame) it.next();
                    if (sf != null) {
                        sf.setIcon(false);
                        sf.setMaximum(true);
                    }
                }
            }
        } catch (Exception ex) {
            LOGGER.error("maximizeMenuItemActionPerformed", ex);
            //do nothing
        }
    }

    private void aboutMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        try {
            JEditorPane jEditorPane = new JEditorPane(myClassLoader.getResource(resourceConverter.getGUIReference("version")).toString());
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
    private void newJDBCExplorerMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        try {
            if (jdbcDriverClassName == null || jdbcDriverClassName.trim().length() == 0 ||
                    jdbcConnectionUrl == null || jdbcConnectionUrl.trim().length() == 0) {
                throw new Exception("Driver=" + this.jdbcDriverClassName + "\nConnection Url=" + this.jdbcConnectionUrl);
            }

            JInternalFrame jf = new JInternalFrame();
            jf.setTitle(getResourceConverter().getFormattedString("workbench.new.JDBCExplorer.title",
                    "JDBC Explorer - {0}",
                    new String[] { jdbcConnectionUrl }));
            //jf.setTitle("JDBC Explorer - " + this.jdbcConnectionUrl);

            Class.forName(jdbcDriverClassName);

            java.sql.Connection conn = null;

            if (jdbcUsername != null && jdbcUsername.length() > 0 &&
                jdbcPassword != null && jdbcPassword.length() > 0) {
                conn = java.sql.DriverManager.getConnection(jdbcConnectionUrl, jdbcUsername, jdbcPassword);
            } else {

                conn = java.sql.DriverManager.getConnection(jdbcConnectionUrl);
            }

            JDBCExplorer jdbce = new JDBCExplorer(conn);

            jf.getContentPane().add(jdbce);
            jf.setBounds(0, 0, 500, 480);
            jf.setClosable(true);
            jf.setIconifiable(true);
            jf.setMaximizable(true);
            jf.setResizable(true);
            jf.setVisible(true);

            desktopPane.add(jf);

            jf.show();
        } catch (Exception ex) {
            JOptionPane.showMessageDialog(this,
                    getResourceConverter().getFormattedString("workbench.new.JDBCExplorer.exception",
                            "Database connection not successful.\n{0}",
                            new String[] { ex.getLocalizedMessage() }),
                            getResourceConverter().getString("workbench.new.JDBCExplorer.exception.title", "Database Connection Error") , JOptionPane.ERROR_MESSAGE);
            LOGGER.error("newJDBCExplorerMenuItemActionPerformed", ex);
        }
    }

    private void toolbarPreferencesButtonActionPerformed(java.awt.event.ActionEvent evt) {
        PreferencesDialog pd = new PreferencesDialog(this, true);
        pd.setJDBCConnectionUrl(jdbcConnectionUrl);
        pd.setJDBCDriverClassName(jdbcDriverClassName);
        pd.setJDBCUsername(jdbcUsername);
        pd.setJDBCPassword(jdbcPassword);
        pd.setDatabaseSchema(jdbcSchema);
        pd.setRequireSchema(requireSchema);

        pd.setVisible(true);

        if (pd.accepted()) {
            jdbcConnectionUrl = pd.getJDBCConnectionUrl();
            jdbcDriverClassName = pd.getJDBCDriverClassName();
            jdbcUsername = pd.getJDBCUsername();
            jdbcPassword = pd.getJDBCPassword();
            jdbcSchema = pd.getDatabaseSchema();
            requireSchema = pd.getRequireSchema();

            workbenchProperties.setProperty("jdbcDriverClassName", jdbcDriverClassName);
            workbenchProperties.setProperty("jdbcConnectionUrl", jdbcConnectionUrl);
            workbenchProperties.setProperty("jdbcUsername", jdbcUsername);
            workbenchProperties.setProperty("jdbcPassword", jdbcPassword);
            workbenchProperties.setProperty("jdbcSchema", jdbcSchema);
            workbenchProperties.setProperty("requireSchema", "" + requireSchema);
            //EC: Enforces the JDBC preferences entered througout all schemas currently opened
            //in the workbench.
            resetWorkbench();
        }
    }


    private void newSchemaMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        MondrianProperties.instance();
        /* user's default directory. This default depends on the operating system.
         * It is typically the "My Documents" folder on Windows, and the
         * user's home directory on Unix.
         */
        File defaultDir = FileSystemView.getFileSystemView().getDefaultDirectory();
        File outputFile;
        do  {
            outputFile = new File(defaultDir, "Schema" + newSchema++ + ".xml");
        } while (outputFile.exists());

        openSchemaFrame(outputFile, true);
    }

    private void newQueryMenuItemActionPerformed(java.awt.event.ActionEvent evt) {

        JMenuItem schemaMenuItem = (JMenuItem) schemaWindowMap.get(desktopPane.getSelectedFrame());

        final JInternalFrame jf = new JInternalFrame();
        jf.setTitle(getResourceConverter().getString("workbench.new.MDXQuery.title", "MDX Query"));
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
        queryMenuItem.setText(getResourceConverter().getFormattedString("workbench.new.MDXQuery.menuitem",
                "{0} MDX",
                new String[] { Integer.toString(windowMenuMapIndex) }));
        queryMenuItem.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
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

        jf.addInternalFrameListener(new InternalFrameAdapter() {
            public void internalFrameClosing(InternalFrameEvent e) {
                mdxWindows.remove(jf);
                jf.dispose();
                windowMenu.remove(queryMenuItem);  // follow this by removing file from schemaWindowMap
                return;
            }
        });

        windowMenu.add(queryMenuItem,-1);
        windowMenu.add(jSeparator3,-1);
        windowMenu.add(cascadeMenuItem,-1);
        windowMenu.add(tileMenuItem,-1);
        windowMenu.add(minimizeMenuItem, -1);
        windowMenu.add(maximizeMenuItem, -1);
        windowMenu.add(closeAllMenuItem,-1);

        qp.setMenuItem(queryMenuItem);
        qp.setSchemaWindowMap(schemaWindowMap);
        qp.setWindowMenuIndex(windowMenuMapIndex++);

        if (schemaMenuItem != null) {
            qp.initConnection(schemaMenuItem.getText());
        } else {
            JOptionPane.showMessageDialog(this,getResourceConverter().getString("workbench.new.MDXQuery.no.selection", "No Mondrian connection. Select a Schema to connect."),
                    getResourceConverter().getString("workbench.new.MDXQuery.no.selection.title", "Alert"), JOptionPane.WARNING_MESSAGE);
        }
    }

    // inform all opened mdx query windows about the list of opened schema files
    private void updateMDXCatalogList() {
        Iterator it = mdxWindows.iterator();
        while (it.hasNext()) {
            JInternalFrame elem = (JInternalFrame)  it.next();
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
        if (jf != null &&
            jf.getContentPane().getComponentCount() > 0 &&
            jf.getContentPane().getComponent(0) instanceof SchemaExplorer) {
            return (SchemaExplorer) jf.getContentPane().getComponent(0);
        }
        return null;
    }

    private void saveAsMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        JInternalFrame jf = desktopPane.getSelectedFrame();

        if (jf != null && jf.getContentPane().getComponent(0) instanceof SchemaExplorer) {
            SchemaExplorer se = (SchemaExplorer) jf.getContentPane().getComponent(0);
            java.io.File schemaFile = se.getSchemaFile();
            java.io.File oldSchemaFile = schemaFile;
            java.io.File suggSchemaFile = new File(schemaFile == null ?  se.getSchema().name.trim() + ".xml" : schemaFile.getName());
            MondrianGuiDef.Schema schema = se.getSchema();
            JFileChooser jfc = new JFileChooser();
            MondrianProperties.instance();

            jfc.setSelectedFile(suggSchemaFile);

            if (jfc.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
                try {
                    schemaFile = jfc.getSelectedFile();
                    if (!oldSchemaFile.equals(schemaFile) && schemaFile.exists()) {  //new file already exists, check for overwrite
                        int answer = JOptionPane.showConfirmDialog(null,
                                getResourceConverter().getFormattedString("workbench.saveAs.schema.confirm",
                                        "{0} schema file already exists. Do you want to replace it?",
                                        new String[] { schemaFile.getAbsolutePath() }),
                                        getResourceConverter().getString("workbench.saveAs.schema.confirm.title", "Save As"), JOptionPane.YES_NO_OPTION);
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

                    XMLOutput out = new XMLOutput(new java.io.FileWriter(jfc.getSelectedFile()));
                    schema.displayXML(out);
                    se.setSchemaFile(schemaFile);
                    se.setTitle();  //sets title of iframe
                    setLastUsed(jfc.getSelectedFile().getName(), jfc.getSelectedFile().toURI().toURL().toString());

                    // update menu item with new file name, then update catalog list for mdx queries
                    JMenuItem sMenuItem = (JMenuItem) schemaWindowMap.get(jf);
                    String mtexttokens[] = sMenuItem.getText().split(" ");
                    sMenuItem.setText(mtexttokens[0] + " " + se.getSchemaFile().getName());
                    updateMDXCatalogList(); // schema menu item updated, now update mdx query windows with updated catallog list
                } catch (Exception ex) {
                    LOGGER.error(ex);
                }
            }
        }
    }

    private void viewXMLMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        JInternalFrame jf = desktopPane.getSelectedFrame();
        boolean oldValue = viewXMLMenuItem.getState();
        if (jf != null && jf.getContentPane().getComponent(0) instanceof SchemaExplorer) {
            SchemaExplorer se = (SchemaExplorer) jf.getContentPane().getComponent(0);
            // call schema explorer's view xml event and update the workbench's view menu accordingly'
            ((JCheckBoxMenuItem) evt.getSource()).setSelected(se.editMode(evt));
            return;
        }
        viewXMLMenuItem.setSelected(! oldValue);
    }

    public void saveMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        JInternalFrame jf = desktopPane.getSelectedFrame();

        // Don't save if nothing there
        if (jf == null || jf.getContentPane() == null) {
            return;
        }

        if (jf.getContentPane().getComponent(0) instanceof SchemaExplorer) {
            SchemaExplorer se = (SchemaExplorer) jf.getContentPane().getComponent(0);

            java.io.File schemaFile = se.getSchemaFile();

            if (se.isNewFile()) {
                saveAsMenuItemActionPerformed(evt);
                return;
            }

            se.setDirty(false);
            se.setDirtyFlag(false);
            se.setTitle();  //sets title of iframe

            MondrianGuiDef.Schema schema = se.getSchema();
            MondrianProperties.instance();
            try {
                XMLOutput out = new XMLOutput(new FileWriter(schemaFile));
                schema.displayXML(out);
                setLastUsed(schemaFile.getName(), schemaFile.toURI().toURL().toString());
            } catch (Exception ex) {
                LOGGER.error("saveMenuItemActionPerformed", ex);
            }
        }
    }

    /**
     * Set last used in properties file
     *
     * @param name
     * @param url
     */
    private void setLastUsed(String name, String url) {
        int match = 4;
        String luName = null;
        String propname = null;
        String lastUsed = "lastUsed";
        String lastUsedUrl = "lastUsedUrl";
        for (int i = 1; i <= 4; i++) {
            propname = lastUsed + i;
            luName = workbenchProperties.getProperty(propname) ;

            if (luName != null && luName.equals(name)) {
                match = i;
                break;
            }
        }

        for (int i = match; i > 1; i--) {
            if (workbenchProperties.getProperty(lastUsed + (i - 1)) != null) {
                workbenchProperties.setProperty(lastUsed + i, workbenchProperties.getProperty(lastUsed + (i - 1)));
                workbenchProperties.setProperty(lastUsedUrl + i, workbenchProperties.getProperty(lastUsedUrl + (i - 1)));
            }
        }

        workbenchProperties.setProperty(LAST_USED1, name);
        workbenchProperties.setProperty(LAST_USED1_URL, url);
        updateLastUsedMenu();
        storeWorkbenchProperties();
    }

    private void updateLastUsedMenu() {
        if (workbenchProperties.getProperty(LAST_USED1) == null) {
            jSeparator2.setVisible(false);
        } else {
            jSeparator2.setVisible(true); }

        if (workbenchProperties.getProperty(LAST_USED1) != null) {
            lastUsed1MenuItem.setVisible(true);
        } else {
            lastUsed1MenuItem.setVisible(false);
        }
        if (workbenchProperties.getProperty(LAST_USED2) != null) {
            lastUsed2MenuItem.setVisible(true);
        } else {
            lastUsed2MenuItem.setVisible(false);
        }
        if (workbenchProperties.getProperty(LAST_USED3) != null) {
            lastUsed3MenuItem.setVisible(true);
        } else {
            lastUsed3MenuItem.setVisible(false);
        }
        if (workbenchProperties.getProperty(LAST_USED4) != null) {
            lastUsed4MenuItem.setVisible(true);
        } else {
            lastUsed4MenuItem.setVisible(false);
        }

        lastUsed1MenuItem.setText(workbenchProperties.getProperty(LAST_USED1));
        lastUsed2MenuItem.setText(workbenchProperties.getProperty(LAST_USED2));
        lastUsed3MenuItem.setText(workbenchProperties.getProperty(LAST_USED3));
        lastUsed4MenuItem.setText(workbenchProperties.getProperty(LAST_USED4));
    }

    private void lastUsed1MenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        try {
            openSchemaFrame(new File(new URI(workbenchProperties.getProperty(LAST_USED1_URL))), false);
        } catch (Exception e) //catch (URISyntaxException e)
        {
            LOGGER.error("lastUsed1MenuItemActionPerformed", e);
        }
    }

    private void lastUsed2MenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        try {
            openSchemaFrame(new File(new URI(workbenchProperties.getProperty(LAST_USED2_URL))), false);
            setLastUsed(workbenchProperties.getProperty(LAST_USED2), workbenchProperties.getProperty(LAST_USED2_URL));
        } catch (URISyntaxException e) {
            LOGGER.error("lastUsed2MenuItemActionPerformed", e);
        }
    }

    private void lastUsed3MenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        try {
            openSchemaFrame(new File(new URI(workbenchProperties.getProperty(LAST_USED3_URL))), false);
            setLastUsed(workbenchProperties.getProperty(LAST_USED3), workbenchProperties.getProperty(LAST_USED3_URL));
        } catch (URISyntaxException e) {
            LOGGER.error("lastUsed3MenuItemActionPerformed", e);
        }
    }

    private void lastUsed4MenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        try {
            openSchemaFrame(new File(new URI(workbenchProperties.getProperty(LAST_USED4_URL))), false);
            setLastUsed(workbenchProperties.getProperty(LAST_USED4), workbenchProperties.getProperty(LAST_USED4_URL));
        } catch (URISyntaxException e) {
            LOGGER.error("lastUsed4MenuItemActionPerformed", e);
        }
    }

    /**
     *
     * @param file
     */
    private void openSchemaFrame(File file, boolean newFile) {
        try {

            if (! newFile) {
                // check if file not already open
                if (checkFileOpen(file)) {
                    return;
                }
                // check if schema file exists
                if (! file.exists()) {
                    JOptionPane.showMessageDialog(this,
                            getResourceConverter().getFormattedString("workbench.open.schema.not.found",
                                    "{0} File not found.",
                                    new String[] { file.getAbsolutePath() }),
                                    getResourceConverter().getString("workbench.open.schema.not.found.title", "Alert"), JOptionPane.WARNING_MESSAGE);
                    return;
                }
                // check if file is writable
                if (! file.canWrite()) {
                    JOptionPane.showMessageDialog(this,
                            getResourceConverter().getFormattedString("workbench.open.schema.not.writeable",
                                    "{0} is not writeable.",
                                    new String[] { file.getAbsolutePath() }),
                                    getResourceConverter().getString("workbench.open.schema.not.found.writeable", "Alert"), JOptionPane.WARNING_MESSAGE);
                    return;
                }
                checkSchemaFile(file);
            }

            final JInternalFrame schemaFrame = new JInternalFrame();
            schemaFrame.setTitle(getResourceConverter().getFormattedString("workbench.open.schema.title",
                    "Schema - {0}",
                    new String[] { file.getName() }));
            //schemaFrame.setTitle("Schema - " + file.getName());
            if (jdbcMetaData == null) {
                jdbcMetaData = new JDBCMetaData(this, jdbcDriverClassName,
                    jdbcConnectionUrl, jdbcUsername, jdbcPassword, jdbcSchema, requireSchema);
            }

            schemaFrame.getContentPane().add(new SchemaExplorer(this, file, jdbcMetaData, newFile, schemaFrame));

            String errorOpening = ((SchemaExplorer) schemaFrame.getContentPane().getComponent(0)).getErrMsg() ;
            if (errorOpening != null) {
                JOptionPane.showMessageDialog(this,
                        getResourceConverter().getFormattedString("workbench.open.schema.error",
                                "Error opening schema - {0}.",
                                new String[] { errorOpening }),
                                getResourceConverter().getString("workbench.open.schema.error.title", "Error"), JOptionPane.ERROR_MESSAGE);
                schemaFrame.setClosed(true);
                return;
            }

            schemaFrame.setBounds(0, 0, 1000, 650);
            schemaFrame.setClosable(true);
            schemaFrame.setIconifiable(true);
            schemaFrame.setMaximizable(true);
            schemaFrame.setResizable(true);
            schemaFrame.setVisible(true);

            desktopPane.add(schemaFrame, javax.swing.JLayeredPane.DEFAULT_LAYER);
            schemaFrame.show();
            schemaFrame.setMaximum(true);

            displayWarningOnFailedConnection();

            final javax.swing.JMenuItem schemaMenuItem = new javax.swing.JMenuItem();
            schemaMenuItem.setText(windowMenuMapIndex++ + " "  + file.getName());
            schemaMenuItem.addActionListener(new java.awt.event.ActionListener() {
                public void actionPerformed(java.awt.event.ActionEvent evt) {
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

            windowMenu.add(schemaMenuItem,0);
            windowMenu.setEnabled(true);

            windowMenu.add(jSeparator3,-1);
            windowMenu.add(cascadeMenuItem,-1);
            windowMenu.add(tileMenuItem,-1);
            windowMenu.add(minimizeMenuItem, -1);
            windowMenu.add(maximizeMenuItem, -1);
            windowMenu.add(closeAllMenuItem,-1);

            // add the file details in menu map
            schemaWindowMap.put(schemaFrame, schemaMenuItem);
            updateMDXCatalogList();

            schemaFrame.setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);

            schemaFrame.addInternalFrameListener(new InternalFrameAdapter() {
                public void internalFrameClosing(InternalFrameEvent e) {
                    if (schemaFrame.getContentPane().getComponent(0) instanceof SchemaExplorer) {
                        SchemaExplorer se = (SchemaExplorer) schemaFrame.getContentPane().getComponent(0);
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
            schemaFrame.addFocusListener(new FocusAdapter() {
                public void focusGained(FocusEvent e) {
                    if (schemaFrame.getContentPane().getComponent(0) instanceof SchemaExplorer) {
                        SchemaExplorer se = (SchemaExplorer) schemaFrame.getContentPane().getComponent(0);
                        viewXMLMenuItem.setSelected(se.isEditModeXML());    // update view menu based on schemaframe who gained focus
                    }
                }

                public void focusLost(FocusEvent e) {
                    if (schemaFrame.getContentPane().getComponent(0) instanceof SchemaExplorer) {
                        SchemaExplorer se = (SchemaExplorer) schemaFrame.getContentPane().getComponent(0);
                        viewXMLMenuItem.setSelected(se.isEditModeXML());  // update view menu based on
                    }
                }

            });
            viewXMLMenuItem.setSelected(false);
        } catch (Exception ex) {
            LOGGER.error("openSchemaFrame", ex);
        }

    }
    private void openMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        JFileChooser jfc = new JFileChooser();
        try {
            jfc.setFileSelectionMode(JFileChooser.FILES_ONLY) ;
            jfc.setFileFilter(new javax.swing.filechooser.FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.getName().toLowerCase().endsWith(".xml")
                    || pathname.isDirectory();
                }
                public String getDescription() {
                    return getResourceConverter().getString("workbench.open.schema.file.type", "Mondrian Schema files (*.xml)");
                }

            });

            String lastUsed = workbenchProperties.getProperty(LAST_USED1_URL);

            if (lastUsed != null) {
                jfc.setCurrentDirectory(new File(new URI(lastUsed)));
            }
        } catch (Exception ex) {
            LOGGER.error("Could not set file chooser", ex);
        }
        MondrianProperties.instance();
        if (jfc.showOpenDialog(this) == JFileChooser.APPROVE_OPTION) {
            try {
                setLastUsed(jfc.getSelectedFile().getName(), jfc.getSelectedFile().toURI().toURL().toString());
            } catch (MalformedURLException e) {
                LOGGER.error(e);
            }

            openSchemaFrame(jfc.getSelectedFile(), false);
        }
    }

    // checks if file already open in schema explorer
    private boolean checkFileOpen(File file) {

        Iterator it = schemaWindowMap.keySet().iterator();  // keys=schemaframes
        while (it.hasNext()) {
            JInternalFrame elem = (JInternalFrame) it.next();
            File f = ((SchemaExplorer) elem.getContentPane().getComponent(0)).getSchemaFile();
            if (f.equals(file)) {
                try {
                    elem.setSelected(true); // make the schema file active
                    return true;
                } catch (Exception ex) {
                    schemaWindowMap.remove(elem); // remove file from map as schema frame does not exist
                    break;
                }
            }
        }
        return false;

    }

    private void resetWorkbench() {
        //EC: Updates the JDBCMetaData for each SchemaExplorer contained in each Schema Frame currently opened based
        //on the JDBC preferences entered.
        Iterator theSchemaFrames = schemaWindowMap.keySet().iterator();
        while (theSchemaFrames.hasNext()) {
            JInternalFrame theSchemaFrame = (JInternalFrame) theSchemaFrames.next();
            SchemaExplorer theSchemaExplorer = (SchemaExplorer) theSchemaFrame.getContentPane().getComponent(0);
            File theFile = theSchemaExplorer.getSchemaFile();
            checkSchemaFile(theFile);
            jdbcMetaData = new JDBCMetaData(this, jdbcDriverClassName,
                    jdbcConnectionUrl, jdbcUsername, jdbcPassword, jdbcSchema, requireSchema);
            theSchemaExplorer.resetMetaData(jdbcMetaData);
            theSchemaFrame.updateUI();
        }
        //EC: If the JDBC preferences entered then display a warning.
        displayWarningOnFailedConnection();
    }

    private void displayWarningOnFailedConnection() {
     // display jdbc connection status warning, if connection is uncsuccessful
        if (jdbcMetaData != null && jdbcMetaData.getErrMsg() != null) {
            JOptionPane.showMessageDialog(this,
                    getResourceConverter().getFormattedString("workbench.open.schema.jdbc.error",
                            "Database connection could not be done.\n{0}\nAll validations related to database will be ignored.",
                            new String[] { jdbcMetaData.getErrMsg() }),
                            getResourceConverter().getString("workbench.open.schema.jdbc.error.title", "Alert"), JOptionPane.WARNING_MESSAGE);
        }
    }

    private void checkSchemaFile(File file) {
        // check if schema file is valid by initiating a mondrian connection
        try {
            // this connection parses the catalog file which if invalid will throw exception
            PropertyList list = new PropertyList();
            list.put("Provider", "mondrian");
            list.put("Jdbc", jdbcConnectionUrl);
            list.put("Catalog", file.toURL().toString());
            list.put("JdbcDrivers", jdbcDriverClassName);
            if (jdbcUsername != null && jdbcUsername.length() > 0) {
                list.put("JdbcUser", jdbcUsername);
            }
            if (jdbcPassword != null && jdbcPassword.length() > 0) {
                list.put("JdbcPassword", jdbcPassword);
            }

            // clear cache before connecting
            AggregationManager.instance().getCacheControl(null).flushSchemaCache();

            DriverManager.getConnection(list, null);
        } catch (Exception ex) {
            LOGGER.error("Exception : Schema file " + file.getAbsolutePath() + " is invalid." + ex.getMessage(), ex);
        } catch (Error err) {
            LOGGER.error("Error : Schema file " + file.getAbsolutePath() + " is invalid." + err.getMessage(), err);
        }
    }


    private void exitMenuItemActionPerformed(java.awt.event.ActionEvent evt) {
        storeWorkbenchProperties();
        closeAllSchemaFrames(true);
    }

    /**
     * parseArgs - parse arguments passed into Workbench.
     *
     * @param args the command line arguments
     *
     * Right now, it's very simple.  Just search through the list
     * of arguments.  If it begins with -f=, then the rest is a file name.
     * Ignore any others.  We can make this more complicated later if we
     * need to.
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
            return getResourceConverter().getFormattedString("workbench.tooltip.error",
                    "No help available for {0}",
                    new String[] { titleName });
        }
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        try {
            Workbench w = new Workbench();
            w.parseArgs(args);
            w.setSize(800, 600);
            // if user specified a file to open, do so now.
            if (w.openFile != null) {
                File f = new File(w.openFile);
                if (f.canRead()) {
                    w.openSchemaFrame(f.getAbsoluteFile(), false); // parameter to indicate this is a new or existing catalog file
                }
            }
            w.setVisible(true);
        } catch (Throwable ex) {
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
    private javax.swing.JSeparator jSeparator4;
    private javax.swing.JMenuItem cutMenuItem;
    private javax.swing.JMenuBar menuBar;
    private javax.swing.JMenuItem saveMenuItem;
    private javax.swing.JMenuItem newJDBCExplorerMenuItem;
    private javax.swing.JMenuItem newJDBCExplorerMenuItem2;
    private javax.swing.JCheckBoxMenuItem viewCubesMenuItem;
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
    private javax.swing.JCheckBoxMenuItem viewMeasuresMenuItem;
    private javax.swing.JMenu editMenu;
    private javax.swing.JMenuItem pasteMenuItem;
    private javax.swing.JMenuItem preferencesMenuItem;
    private javax.swing.JCheckBoxMenuItem viewDimensionsMenuItem;
    private javax.swing.JCheckBoxMenuItem viewXMLMenuItem;
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


    /*
class SchemaFrameComp implements Comparator {
  public int compare (Object o1, Object o2) {
    return o1 == o2;
  }
}
     **/

    public javax.swing.JCheckBoxMenuItem getViewXMLMenuItem() {
        return viewXMLMenuItem; // used by schema framewhen it uses 'view xml' to update view xml menu item
    }


}

// End Workbench.java
