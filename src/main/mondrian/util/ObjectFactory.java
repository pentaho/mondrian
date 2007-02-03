/*
// $Id$
// This software is subject to the terms of the Common Public License
// Agreement, available at the following URL:
// http://www.opensource.org/licenses/cpl.html.
// Copyright (C) 2002-2002 Kana Software, Inc.
// Copyright (C) 2002-2006 Julian Hyde and others
// All Rights Reserved.
// You must accept the terms of that agreement to use this software.
//
// jhyde, Dec 23, 2002
*/
package mondrian.util;

import java.lang.reflect.Constructor;
import java.util.Properties;

/** 
 * Concrete derived classes of the generic <code>ObjectFactory</code> class
 * are used to produce an implementation of an interface. In general, a
 * factory should produce a default implementation for general application
 * use as well as particular implementations used during testing.
 * During testing of application code and during normal execution,
 * the application code uses one of the <code>ObjectFactory</code>'s
 * methods for producing implementation instances - the same method is
 * used both for test and non-test modes. There are two ways of
 * modifying the implementation returned to the application code.
 * The first is for the application to use Properties.
 * The <code>ObjectFactory</code> implementation looks for a given
 * property (by default the name of the property is the class name
 * of the interfaceClass object) and if found uses it as the classname 
 * to create. 
 * A second approach is to use a ThreadLocal; if the ThreadLocal 
 * is non-empty then use it as the classname.
 * 
 * @author <a>Richard M. Emberson</a>
 * @since Feb 01 2007
 * @version $Id$
 */
public abstract class ObjectFactory<V> {
    
    public static final Class[] EMPTY_CLASS_ARRAY = new Class[0];
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    /** 
     * The type of the object to be generated. 
     */
    private final Class<V> interfaceClass;

    /** 
     * Create a new object. The <code>interfaceClass</code> parameter 
     * is used to cast the object generated to type right type.
     * 
     * @param final Class<V>interfaceClass 
     */
    protected ObjectFactory(final Class<V>interfaceClass) {
        this.interfaceClass = interfaceClass;
    }

    /** 
     * Construct an object where the System Properties can be used
     * to look up a class name. 
     * The constructor for the object takes no parameters.
     * 
     * @return 
     * @throws CreationException 
     */
    public final V getObject() throws CreationException {
        return getObject(System.getProperties());
    }
    
    /** 
     * Construct an object where the <code>Properties</code> parameter can 
     * be used to look up a class name.
     * The constructor for the object takes no parameters.
     * 
     * @param props 
     * @return 
     * @throws CreationException 
     */
    public final V getObject(final Properties props) throws CreationException {
        return getObject(props, EMPTY_CLASS_ARRAY, EMPTY_OBJECT_ARRAY);
    }
    
    /** 
     * Construct an object where the <code>parameterTypes</code> and
     * <code>parameterValues</code> are constructor parameters and
     * System Properties are used to look up a class name.
     * 
     * @param parameterTypes 
     * @param parameterValues 
     * @return 
     * @throws CreationException 
     */
    public final V getObject(final Class[] parameterTypes,
                             final Object[] parameterValues)
            throws CreationException {
        return getObject(System.getProperties(),
                         parameterTypes,
                         parameterValues);
    }

    /** 
     * Construct an object where the <code>parameterTypes</code> and
     * <code>parameterValues</code> are constructor parameters and
     * Properties parameter is used to look up a class name.
     * 
     * @param props 
     * @param parameterTypes 
     * @param parameterValues 
     * @return 
     * @throws CreationException 
     */
    public final V getObject(final Properties props, 
                             final Class[] parameterTypes,
                             final Object[] parameterValues)
            throws CreationException {

        String className = getClassName();
        if (className == null) {
            className = getClassName(props);
        }

        V result = null;

        if (className != null) {
            try {
                final ClassLoader loader =
                    Thread.currentThread().getContextClassLoader();
                final Class<?> genericClass =
                    Class.forName(className, true, loader);
                final Class<? extends V> specificClass =
                    asSubclass(interfaceClass, genericClass);
                final Constructor<? extends V> constructor =
                    specificClass.getConstructor(parameterTypes);

                result = constructor.newInstance(parameterValues);
            } catch (Exception exc) {
                throw new CreationException(
                            "Error creating object of type \"" +
                                            getClass().getName() + "\"" ,
                                            exc);
            }
        } else {
            result = getDefault(parameterTypes, parameterValues);
        }

        return result;
    }

    
    /** 
     * This is a a back port of a 1.5 version Class method.
     * 
     * @param clazz 
     * @param genericClass 
     * @return 
     */
    private <V> Class<? extends V> asSubclass(final Class<V> clazz, 
                                              final Class<?> genericClass) {
        if (clazz.isAssignableFrom(genericClass))
            return (Class<? extends V>) genericClass;
        else
            throw new ClassCastException(genericClass.toString());
    }

    
    /** 
     * Return the name of a class to use to create an object. 
     * The default implementation returns null but derived
     * classes can return a class name.
     * 
     * @return 
     */
    protected String getClassName() {
        return null;
    }

    /** 
     * Return the name of a class to use to create an object. 
     * By default the name returned is the value of a property
     * with key equal to the class name of the <code>interfaceClass</code>.
     * This method is allowed to return null.
     * 
     * @return 
     */
    protected String getClassName(final Properties props) {
        return props.getProperty(interfaceClass.getName());
    }

    /** 
     * For most uses (other than testing) this is the method that derived
     * classes implement that return the desired object.
     * 
     * @param parameterTypes 
     * @param parameterValues 
     * @return 
     * @throws CreationException 
     */
    protected abstract V getDefault(Class[] parameterTypes,
                                    Object[] parameterValues)
        throws CreationException;

    /** 
     * This method can be used by derived classes to throw a
     * CreationException if an object can not be created.
     * 
     * @return 
     */
    protected CreationException defaultCreationException() {
        return new CreationException("Error creating object of type \"" +
                                            getClass().getName() + "\"");
    }
}
