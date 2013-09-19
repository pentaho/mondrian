/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2013 Pentaho Corporation..  All rights reserved.
*/

package mondrian.util;

import org.eigenbase.util.property.StringProperty;

import java.lang.reflect.*;
import java.util.Properties;

/**
 * Concrete derived classes of the generic <code>ObjectFactory</code> class
 * are used to produce an implementation of an interface (a
 * normal interface implementation or a Proxy). In general, a
 * factory should produce a default implementation for general application
 * use as well as particular implementations used during testing.
 * During testing of application code and during normal execution,
 * the application code uses one of the <code>ObjectFactory</code>'s
 * methods for producing implementation instances - the same method is
 * used both for test and non-test modes. There are two ways of
 * modifying the implementation returned to the application code.
 *
 * <p>The first is for the application to use Properties.
 * The <code>ObjectFactory</code> implementation looks for a given
 * property (by default the name of the property is the class name
 * of the interfaceClass object) and if found uses it as the classname
 * to create.</p>
 *
 * <p>A second approach is to use a ThreadLocal; if the ThreadLocal
 * is non-empty then use it as the class name.</p>
 *
 * <p>When to use a Factory?
 *
 * <p>Everyone has an opinion. For me, there are two criteria: enabling
 * unit testing and providing end-user/developer-customizer overriding.
 *
 * <p>If a method has side-effects, either its result depends upon
 * a side-effect or calling it causes a side-effect, then the Object
 * hosting the method is a candidate for having a factory. Why?
 * Well, consider the case where a method returns the value of
 * a System property and the System property is determined only once
 * and set to a static final variable:
 *
 * <pre>
 *      class OneValue {
 *          private static final boolean propValue;
 *          static {
 *              propValue = Boolean.getBoolean("com.app.info.value");
 *          }
 *          .....
 *          public boolean hasInfo() {
 *              return propValue;
 *          }
 *      }
 * </pre>
 *
 * <p>In this case, only one value is ever returned. If you have a
 * module, a client of the above code, that uses the value returned
 * by a call to the
 * <code>hasInfo()</code> method, how do you write a unit test of
 * your module that tests both possible return values?
 * You can not, its value is based upon a side-effect, an external
 * value that can not be controlled by the unit test.</p>
 *
 * <p>If the <code>OneValue</code> class was an interface and there was a
 * factory, then the unit test could arrange that its own version of the
 * <code>OneValue</code>
 * interface was returned and in one test arrange that <code>true</code>
 * was returned and in a second test, arrange that <code>false</code>
 * was returned.</p>
 *
 * <p>The above is a trivial example of code that disallows clients of the
 * code from being properly tested.</p>
 *
 * <p>Another example might be a module that directly initializes a JMS
 * queue and receives JMS message
 * from the JMS queue. This code can not be tested without having a live
 * JMS queue. On the other hand, if one defines an interface allowing
 * one to wrap access to the JMS queue and accesses the implementation
 * via a factory, then unit tests can be create that use a mock
 * JMS queue.</p>
 *
 * <p>With regards to providing end-user/developer-customizer overriding,
 * its generally good to have a flexible application framework.
 * Experimental or just different implementations can be developed and
 * tested without having to touch a lot of the application code itself.</p>
 *
 * <p>There is, of course, a trade-off between the use of a factory
 * and the size or simplicity of the object being created.</p>
 *
 * <p>What are the requirements for a template ObjectFactory?</p>
 *
 * <p>First, every implementation must support the writing of unit tests.
 * What this means it that test cases can override what the factory
 * produces. The test cases can all use the same produced Object or
 * each can request an Object targeted to its particular test. All this
 * without changing the <code>default</code> behavior of the factory.</p>
 *
 * <p>Next, it should be possible to create a factory from the template that
 * is intended to deliver the same Object each time it is called, a
 * different, new Object each time it is called, or, based on the
 * calling environment (parameters, properties, <code>ThreadLocal</code>,
 * etc.) one of a set of Objects. These are possible <code>default</code>
 * behaviors, but, again, they can be overridden for test purposes.</p>
 *
 * <p>While a factory has a <code>default</code> behavior in an
 * application, it must be possible for every factory's behavior
 * in that application to be globally overridden. What that means is
 * if the application designer has dictated a <code>default</code>, the
 * application user should be able to change the default. An example of
 * this is overriding what Object is returned based upon a
 * <code>System</code> property value.</p>
 *
 * <p>Lastly, every factory is a singleton - if an interface with
 * an implementation whose creation is mediated by a factory, then
 * there is a single factory that does that creating.
 * This does not mean that such a factory always return the same value,
 * rather that there is only one instance of the factory itself.</p>
 *
 * <p>The following is an example class that generates a factory
 * singleton. In this case, the factory extends the
 * <code>ObjectFactory</code>
 * rather than the <code>ObjectFactory.Singleton</code>:</p>
 *
 * <pre>
 *
 *      public final class FooFactory extends ObjectFactory<Foo> {
 *          // The single instance of the factory
 *          private static final FooFactory factory;
 *          static {
 *              factory = new FooFactory();
 *          }
 *          public static FooFactory instance() {
 *              return factory;
 *          }
 *          ..........
 *          private FooFactory() {
 *              super(Foo.class);
 *          }
 *          ..........
 *      }
 *
 * </pre>
 *
 * <p>There are multiple ways of creating derived classes that have support
 * for unit testing. A very simple way is to use <code>ThreadLocal</code>s.</p>
 *
 * <pre>
 *
 *          private static final ThreadLocal ClassName = new ThreadLocal();
 *          private static String getThreadLocalClassName() {
 *              return (String) ClassName.get();
 *          }
 *          public static void setThreadLocalClassName(String className) {
 *              ClassName.set(className);
 *          }
 *          public static void clearThreadLocalClassName() {
 *              ClassName.set(null);
 *          }
 *          ..........
 *          protected String getClassName() {
 *              return getThreadLocalClassName();
 *          }
 *
 * </pre>
 *
 * <p>Here, the unit test will call the <code>setThreadLocalClassName</code>
 * method setting it with the class name of a specialized implementation of
 * the template interface. In the <code>finally</code> clause of the
 * unit test, it is very important that there be a call to the
 * <code>clearThreadLocalClassName</code> method so that other
 * tests, etc. do not get an instance of the test-specific specialized
 * implementation.</p>
 *
 * <p>The following is an example unit test that uses the factory's
 * <code>ThreadLocal</code> to override the implementation that is returned.</p>
 *
 * <pre>
 *      interface Boo {
 *          boolean getValue();
 *          .......
 *      }
 *      class NormalBooImpl implements Boo {
 *          public boolean getValue() { ... }
 *          .......
 *      }
 *      class MyCode {
 *          private Boo boo;
 *          MyCode() {
 *              boo = BooFactory.instance().getObject();
 *          }
 *          .......
 *          int getValue() {
 *              if (boo.getValue()) {
 *                  return 1;
 *              } else {
 *                  return 0;
 *              }
 *
 *          }
 *      }
 *
 *      class MyCodeTest {
 *          private static boolean testValue;
 *          static class BooTest1 implements Boo {
 *              public boolean getValue() {
 *                  return MyTest.testValue;
 *              }
 *              .....
 *          }
 *          static class BooTest2 implements
 *                      java.lang.reflect.InvocationHandler {
 *              private final Boo boo;
 *              public BooTest2() {
 *                  // remove test class name
 *                  BooFactory.clearThreadLocalClassName();
 *                  // get default Boo implementation
 *                  this.boo = BooFactory.instance().getObject();
 *              }
 *              public Object invoke(Object proxy, Method method, Object[] args)
 *                  throws Throwable {
 *                  if (method.getName().equals("getValue")) [
 *                      return new Boolean(MyTest.testValue);
 *                  } else {
 *                      return method.invoke(this.boo, args);
 *                  }
 *              }
 *          }
 *          public void test1() {
 *              try {
 *                  // Factory will creates test class
 *                  BooFactory.setThreadLocalClassName("MyTest.BooTest1");
 *
 *                  MyTest.testValue = true;
 *                  MyCode myCode = new MyCode();
 *                  int value = myCode.getValue();
 *                  assertTrue("Value not 1", (value == 1));
 *
 *                  MyTest.testValue = false;
 *                  myCode = new MyCode();
 *                  value = myCode.getValue();
 *                  assertTrue("Value not 0", (value == 0));
 *              } finally {
 *                  BooFactory.clearThreadLocalClassName();
 *              }
 *          }
 *          public void test2() {
 *              try {
 *                  // Use InvocationHandler and Factory Proxy capability
 *                  BooFactory.setThreadLocalClassName("MyTest.BooTest2");
 *
 *                  MyTest.testValue = true;
 *                  MyCode myCode = new MyCode();
 *                  int value = myCode.getValue();
 *                  assertTrue("Value not 1", (value == 1));
 *
 *                  MyTest.testValue = false;
 *                  myCode = new MyCode();
 *                  value = myCode.getValue();
 *                  assertTrue("Value not 0", (value == 0));
 *              } finally {
 *                  BooFactory.clearThreadLocalClassName();
 *              }
 *          }
 *      }
 *
 * </pre>
 *
 * <p>While this is a very simple example, it shows how using such factories
 * can aid in creating testable code. The MyCode method is a client of
 * the Boo implementation. How to test the two different code branches the
 * method can take? Because the Boo object is generated by a factory,
 * one can override what object the factory returns.</p>
 *
 * @author Richard M. Emberson
 * @since Feb 01 2007
 */
public abstract class ObjectFactory<V> {

    private static final Class[] EMPTY_CLASS_ARRAY = new Class[0];
    private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];

    /**
     * The type of the object to be generated.
     */
    private final Class<V> interfaceClass;

    /**
     * Creates a new factory object. The <code>interfaceClass</code> parameter
     * is used to cast the object generated to type right type.
     *
     * @param interfaceClass the class object for the interface implemented
     * by the objects returned by this factory
     *
     */
    protected ObjectFactory(final Class<V> interfaceClass) {
        this.interfaceClass = interfaceClass;
    }

    /**
     * Constructs an object where the System Properties can be used
     * to look up a class name.
     * The constructor for the object takes no parameters.
     *
     * @return the newly created object
     * @throws CreationException if unable to create the object
     */
    protected final V getObject() throws CreationException {
        return getObject(System.getProperties());
    }

    /**
     * Constructs an object where the <code>Properties</code> parameter can
     * be used to look up a class name.
     * The constructor for the object takes no parameters.
     *
     * @param props the property definitions to use to determine the
     * implementation class
     *
     * @return the newly created object
     * @throws CreationException if unable to create the object
     */
    protected final V getObject(final Properties props)
        throws CreationException
    {
        return getObject(props, EMPTY_CLASS_ARRAY, EMPTY_OBJECT_ARRAY);
    }

    /**
     * Constructs an object where the <code>parameterTypes</code> and
     * <code>parameterValues</code> are constructor parameters and
     * System Properties are used to look up a class name.
     *
     * @param parameterTypes  the class parameters that define the signature
     * of the constructor to use
     * @param parameterValues  the values to use to construct the current
     * instance of the object
     * @return the newly created object
     * @throws CreationException if unable to create the object
     */
    protected final V getObject(
        final Class[] parameterTypes,
        final Object[] parameterValues)
        throws CreationException
    {
        return getObject(
            System.getProperties(), parameterTypes, parameterValues);
    }

    /**
     * Constructs an object where the <code>parameterTypes</code> and
     * <code>parameterValues</code> are constructor parameters and
     * Properties parameter is used to look up a class name.
     * <p>
     * This returns a new instance of the Object each time its
     * called (assuming that if the method <code>getDefault</code>,
     * which derived classes implement), if called, creates a new
     * object each time.
     *
     * @param props the property definitions to use to determine the
     * @param parameterTypes  the class parameters that define the signature
     * of the constructor to use
     * @param parameterValues  the values to use to construct the current
     * instance of the object
     * @return the newly created object
     * @throws CreationException if unable to create the object
     */
    protected V getObject(
        final Properties props,
        final Class[] parameterTypes,
        final Object[] parameterValues) throws CreationException
    {
        // Unit test override
        final String className = getClassName();
        if (className != null) {
            return getObject(className, parameterTypes, parameterValues);
        }

        final String propClassName = getClassName(props);
        return (propClassName != null)
            // User overriding application default
            ? getObject(propClassName, parameterTypes, parameterValues)
            // Get application default
            : getDefault(parameterTypes, parameterValues);
    }

    /**
     * Creates an instance with the given <code>className</code>,
     * <code>parameterTypes</code> and <code>parameterValues</code> or
     * throw a <code>CreationException</code>. There are two different
     * mechanims available. The first is to uses reflection
     * to create the instance typing the generated Object based upon
     * the <code>interfaceClass</code> factory instance object.
     * With the second the <code>className</code> is an class that implements
     * the <code>InvocationHandler</code> interface and in this case
     * the <code>java.lang.reflect.Proxy</code> class is used to
     * generate a proxy.
     *
     * @param className the class name used to create Object instance
     * @param parameterTypes  the class parameters that define the signature
     * of the constructor to use
     * @param parameterValues  the values to use to construct the current
     * instance of the object
     * @return the newly created object
     * @throws CreationException if unable to create the object
     */
    protected V getObject(
        final String className,
        final Class[] parameterTypes,
        final Object[] parameterValues) throws CreationException
    {
        try {
            // As a place to begin google:
            //   org.apache.cxf.BusFactoryHelper.java
            final Class<?> genericClass =
                ClassResolver.INSTANCE.forName(className, true);

            // Are we creating a Proxy or an instance?
            if (InvocationHandler.class.isAssignableFrom(genericClass)) {
                final Constructor constructor =
                    genericClass.getConstructor(parameterTypes);
                InvocationHandler handler = (InvocationHandler)
                    constructor.newInstance(parameterValues);
                //noinspection unchecked
                return (V) Proxy.newProxyInstance(
                    genericClass.getClassLoader(),
                    new Class[] { this.interfaceClass },
                    handler);
            } else {
                final Class<? extends V> specificClass =
                    genericClass.asSubclass(interfaceClass);
                final Constructor<? extends V> constructor =
                    specificClass.getConstructor(parameterTypes);

                return constructor.newInstance(parameterValues);
            }
        } catch (Exception exc) {
            throw new CreationException(
                "Error creating object of type \""
                    + this.interfaceClass.getName() + "\"",
                exc);
        }
    }

    /**
     * Returns the name of a class to use to create an object.
     * The default implementation returns null but derived
     * classes can return a class name.
     * <p>
     * This method is the primary mechanism for supporting Unit testing.
     * A derived class can have, as an example, this method return
     * the value of a <code>ThreadLocal</code>. For testing it
     * return a class name while for normal use it returns <code>null</code>.
     *
     * @return <code>null</code> or a class name
     */
    protected String getClassName() {
        return null;
    }

    /**
     * Returns the name of a class to use to create an object.
     * The factory's <code>StringProperty</code> is gotten and
     * if it has a non-null value, then that is returned. Otherwise,
     * the <code>StringProperty</code>'s name (path) is used as the
     * name to probe the <code>Properties</code> object for a value.
     * This method is allowed to return null.
     *
     * @return <code>null</code> or a class name
     */
    protected String getClassName(final Properties props) {
        final StringProperty stringProp = getStringProperty();
        final String className = stringProp.get();
        return (className != null)
            ? className
            : (props == null)
            ? null : props.getProperty(stringProp.getPath());
    }

    /**
     * Return the <code>StringProperty</code> associated with this factory.
     *
     * @return the  <code>StringProperty</code>
     */
    protected abstract StringProperty getStringProperty();

    /**
     * For most uses (other than testing) this is the method that derived
     * classes implement that return the desired object.
     *
     * @param parameterTypes  the class parameters that define the signature
     * of the constructor to use
     * @param parameterValues  the values to use to construct the current
     * instance of the object
     * @return the newly created object
     * @throws CreationException if unable to create the object
     */
    protected abstract V getDefault(
        Class[] parameterTypes,
        Object[] parameterValues)
        throws CreationException;

    /**
     * Gets the current override values in the opaque context object and
     * clears those values within the Factory.
     *
     * <p>This is used in testing.
     *
     * @return the test <code>Context</code> object.
     */
    public Object removeContext() {
        return null;
    }

    /**
     * Restores the context object resetting override values.
     *
     * <p>This is used in testing.
     *
     * @param context the context object to be restored.
     */
    public void restoreContext(final Object context) {
        // empty
    }


    /**
     * Implementation of ObjectFactory
     * that returns only a single instance of the Object.
     */
    public static abstract class Singleton<T> extends ObjectFactory<T> {

        /**
         * The single instance of the object created by the factory.
         */
        protected T singleInstance;

        /**
         * The test single instance of the object created by the factory.
         * Creating this <code>testSingleInstance</code> does not change the
         * current value of the <code>singleInstance</code> variable.
         */
        protected T testSingleInstance;

        /**
         * Creates a new singleton factory object. The
         * <code>interfaceClass</code> parameter
         * is used to cast the object generated to type right type.
         *
         * @param interfaceClass the class object for the interface implemented
         * by the objects returned by this factory
         */
        protected Singleton(final Class<T> interfaceClass) {
            super(interfaceClass);
        }

        /**
         * Returns the singleton Object.
         * The first time this is called, an object is created where
         * the <code>parameterTypes</code> and
         * <code>parameterValues</code> are constructor parameters and
         * Properties parameter is used to look up a class name.
         * <p>
         * This returns a same instance of the Object each time its
         * called except if the <code>getClassName</code> method
         * returns a non-null class name which should only
         * happen as needed for unit testing.
         *
         * @param props the property definitions to use to determine the
         * @param parameterTypes  the class parameters that define the signature
         * of the constructor to use
         * @param parameterValues  the values to use to construct the current
         * instance of the object
         * @return the newly created object
         * @throws CreationException if unable to create the object
         */
        protected T getObject(
            final Properties props,
            final Class[] parameterTypes,
            final Object[] parameterValues) throws CreationException
        {
            // Unit test override, do not use application instance.
            final String className = getClassName();
            if (className != null) {
                if (this.testSingleInstance == null) {
                    this.testSingleInstance =
                        getTestObject(
                            className,
                            parameterTypes,
                            parameterValues);
                }
                return this.testSingleInstance;
            }

            // NOTE: Should we distinguish between any Properties Object
            // and that returned by System? When its the System's
            // Properties Object (which is not a final instance variable
            // within the System class), then its for sure the user
            // providing a global override. If its not the System
            // Properties object, then it may or may not be a global
            // override so we may not want to set the singleInstance
            // to it. For now I am ignoring the issue.
            if (this.singleInstance == null) {
                final String propClassName = getClassName(props);

                this.singleInstance = (propClassName != null)
                    // The user overriding application default
                    ? getObject(propClassName, parameterTypes, parameterValues)
                    // Get application default
                    : getDefault(parameterTypes, parameterValues);
            }
            return this.singleInstance;
        }

        /**
         * Create an instance for test purposes.
         *
         * @param className the class name used to create Object instance
         * @param parameterTypes  the class parameters that define the signature
         * of the constructor to use
         * @param parameterValues  the values to use to construct the current
         * instance of the object
         * @return the newly created object
         * @throws CreationException if unable to create the object
         */
        protected T getTestObject(
            final String className,
            final Class[] parameterTypes,
            final Object[] parameterValues) throws CreationException
        {
            return getObject(className, parameterTypes, parameterValues);
        }
    }

    /**
     * This is for testing only.
     * <p>
     * <code>Context</code> contain the Factory implementation specific
     * non-default values and mechanism for overriding the default
     * instance type returned by the Factory.
     * Factory implementation can extend the <code>Context</code> interface
     * to capture its specific override values.
     * If, for example, a Factory implementation uses a <code>ThreadLocal</code>
     * to override the default instance type for unit tests, then the
     * <code>Context</code>
     * will hold the current value of the <code>ThreadLocal</code>.
     * Getting the Context, clears the <code>ThreadLocal</code> value.
     * This allows the tester who wishes to create code that will provide
     * a wrapper around the default instance type to register their
     * wrapper class name with the <code>ThreadLocal</code>, and, within
     * the wrapper constructor, get the <code>Context</code>, get
     * a default instance, and then restore the <code>Context</code>.
     */
    public interface Context {
    }
}

// End ObjectFactory.java
