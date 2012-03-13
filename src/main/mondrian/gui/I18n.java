/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2007-2007 JasperSoft
// Copyright (C) 2008-2009 Pentaho
// All Rights Reserved.
*/
package mondrian.gui;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.text.MessageFormat;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class I18n {
    private static final Logger LOGGER = Logger.getLogger(I18n.class);

    // Default to english
    private Locale currentLocale = Locale.ENGLISH;

    private ResourceBundle guiBundle = null;
    private ResourceBundle languageBundle = null;

    private static String defaultIcon = "nopic";

    private static List<LanguageChangedListener> languageChangedListeners =
        new ArrayList<LanguageChangedListener>();

    public static void addOnLanguageChangedListener(
        LanguageChangedListener listener)
    {
        languageChangedListeners.add(listener);
    }

    public I18n(ResourceBundle guiBundle, ResourceBundle languageBundle) {
        this.guiBundle = guiBundle;
        this.languageBundle = languageBundle;
    }

    public static List<Locale> getListOfAvailableLanguages(Class cl) {
        List<Locale> supportedLocales = new ArrayList<Locale>();

        try {
            Set<String> names = getResourcesInPackage(cl, cl.getName());
            for (String name : names) {
                // From
                //    '../../<application>_en.properties'
                //   or
                //    '../../<application>_en_UK.properties'
                // To
                // 'en' OR 'en_UK_' OR even en_UK_Brighton dialect

                String lang = name.substring(name.lastIndexOf('/') + 1);

                // only accept resources with extension '.properties'
                if (lang.indexOf(".properties") < 0) {
                    continue;
                }

                lang = lang.substring(0, lang.indexOf(".properties"));

                StringTokenizer tokenizer = new StringTokenizer(lang, "_");
                if (tokenizer.countTokens() <= 1) {
                    continue;
                }

                String language = "";
                String country = "";
                String variant = "";

                int i = 0;
                while (tokenizer.hasMoreTokens()) {
                    String token = tokenizer.nextToken();

                    switch (i) {
                    case 0:
                        //the word <application>
                        break;
                    case 1:
                        language = token;
                        break;
                    case 2:
                        country = token;
                        break;
                    case 3:
                        variant = token;
                        break;
                    default:
                        //
                    }
                    i++;
                }

                Locale model = new Locale(language, country, variant);
                supportedLocales.add(model);
            }
        } catch (Exception e) {
            LOGGER.error("getListOfAvailableLanguages", e);
        }

        // Sort the list. Probably should use the current locale when getting
        // the DisplayLanguage so the sort order is correct for the user.

        Collections.sort(
            supportedLocales,
            new Comparator<Object>() {
                public int compare(Object lhs, Object rhs) {
                    String ls = ((Locale) lhs).getDisplayLanguage();
                    String rs = ((Locale) rhs).getDisplayLanguage();

                    // TODO: this is not very nice - We should introduce a
                    // MyLocale
                    if (ls.equals("pap")) {
                        ls = "Papiamentu";
                    }
                    if (rs.equals("pap")) {
                        rs = "Papiamentu";
                    }

                    return ls.compareTo(rs);
                }
            });

        return supportedLocales;
    }

    /**
     * Enumerates the resouces in a give package name.
     * This works even if the resources are loaded from a jar file!
     *
     * <p/>Adapted from code by mikewse
     * on the java.sun.com message boards.
     * http://forum.java.sun.com/thread.jsp?forum=22&thread=30984
     *
     * <p>The resulting set is deterministically ordered.
     *
     * @param coreClass   Class for class loader to find the resources
     * @param packageName The package to enumerate
     * @return A Set of Strings for each resouce in the package.
     */
    public static Set<String> getResourcesInPackage(
        Class coreClass,
        String packageName)
        throws IOException
    {
        String localPackageName;
        if (packageName.endsWith("/")) {
            localPackageName = packageName;
        } else {
            localPackageName = packageName + '/';
        }

        ClassLoader cl = coreClass.getClassLoader();
        Enumeration<URL> dirEnum = cl.getResources(localPackageName);
        Set<String> names = new LinkedHashSet<String>(); // deterministic
        while (dirEnum.hasMoreElements()) {
            URL resUrl = dirEnum.nextElement();

            // Pointing to filesystem directory
            if (resUrl.getProtocol().equals("file")) {
                try {
                    File dir = new File(resUrl.getFile());
                    File[] files = dir.listFiles();
                    if (files != null) {
                        for (int i = 0; i < files.length; i++) {
                            File file = files[i];
                            if (file.isDirectory()) {
                                continue;
                            }
                            names.add(localPackageName + file.getName());
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

                // Pointing to Jar file
            } else if (resUrl.getProtocol().equals("jar")) {
                JarURLConnection jconn =
                    (JarURLConnection) resUrl.openConnection();
                JarFile jfile = jconn.getJarFile();
                Enumeration entryEnum = jfile.entries();
                while (entryEnum.hasMoreElements()) {
                    JarEntry entry = (JarEntry) entryEnum.nextElement();
                    String entryName = entry.getName();
                    // Exclude our own directory
                    if (entryName.equals(localPackageName)) {
                        continue;
                    }
                    String parentDirName =
                        entryName.substring(0, entryName.lastIndexOf('/') + 1);
                    if (!parentDirName.equals(localPackageName)) {
                        continue;
                    }
                    names.add(entryName);
                }
            } else {
                // Invalid classpath entry
            }
        }

        return names;
    }


    public void setCurrentLocale(String language) {
        setCurrentLocale(language, null);
    }

    public void setCurrentLocale(String language, String country) {
        if (language != null && !language.equals("")) {
            if (country != null && !country.equals("")) {
                setCurrentLocale(new Locale(language, country));
            } else {
                setCurrentLocale(new Locale(language));
            }
        } else {
            setCurrentLocale(Locale.getDefault());
        }
    }

    public void setCurrentLocale(Locale locale) {
        currentLocale = locale;
        languageBundle = null;

        for (LanguageChangedListener listener : languageChangedListeners) {
            try {
                listener.languageChanged(new LanguageChangedEvent(locale));
            } catch (Exception ex) {
                LOGGER.error("setCurrentLocale", ex);
            }
        }
    }

    public Locale getCurrentLocale() {
        if (currentLocale == null) {
            currentLocale = Locale.getDefault();
        }
        return currentLocale;
    }

    public String getGUIReference(String reference) {
        try {
            if (guiBundle == null) {
                throw new Exception("No GUI bundle");
            }
            return guiBundle.getString(reference);
        } catch (MissingResourceException ex) {
            LOGGER.error(
                "Can't find the translation for key = " + reference, ex);
            throw ex;
        } catch (Exception ex) {
            LOGGER.error("Exception loading reference = " + reference, ex);
            return guiBundle.getString(defaultIcon);
        }
    }

    /**
     * Retreives a resource string using the current locale.
     *
     * @param stringId The resource string identifier
     * @return The locale specific string
     */
    public String getString(String stringId) {
        return getString(stringId, getCurrentLocale());
    }

    /**
     * Retreives a resource string using the current locale, with a default.
     *
     * @param stringId     The resource string identifier
     * @param defaultValue If no resource for the stringID is specified, use
     *                     this default value
     * @return The locale specific string
     */
    public String getString(String stringId, String defaultValue) {
        return getString(stringId, getCurrentLocale(), defaultValue);
    }

    /**
     * Retrieves a resource string using the current locale.
     *
     * @param stringId     The resource string identifier
     * @param defaultValue The default value for the resource string
     * @param args         arguments to be inserted into the resource string
     * @return The locale specific string
     */
    public String getFormattedString(
        String stringId,
        String defaultValue,
        Object... args)
    {
        String pattern = getString(stringId, getCurrentLocale(), defaultValue);
        MessageFormat mf =
            new java.text.MessageFormat(pattern, getCurrentLocale());
        return mf.format(args);
    }

    /**
     * Retreive a resource string using the given locale. The stringID is the
     * default.
     *
     * @param stringId      The resource string identifier
     * @param currentLocale required Locale for resource
     * @return The locale specific string
     */
    private String getString(String stringId, Locale currentLocale) {
        return getString(stringId, currentLocale, stringId);
    }

    /**
     * Retreive a resource string using the given locale. Use the default if
     * there is nothing for the given Locale.
     *
     * @param stringId      The resource string identifier
     * @param currentLocale required Locale for resource
     * @param defaultValue  The default value for the resource string
     * @return The locale specific string
     */
    public String getString(
        String stringId,
        Locale currentLocale,
        String defaultValue)
    {
        try {
            if (languageBundle == null) {
                throw new Exception("No language bundle");
            }
            return languageBundle.getString(stringId);
        } catch (MissingResourceException ex) {
            LOGGER.error(
                "Can't find the translation for key = "
                + stringId
                + ": using default ("
                + defaultValue
                + ")", ex);
        } catch (Exception ex) {
            LOGGER.error("Exception loading stringID = " + stringId, ex);
        }
        return defaultValue;
    }

    public static String getCurrentLocaleID() {
        return "";
    }
}

// End I18n.java
