package mondrian.xmla;

import mondrian.olap.Util;
import mondrian.olap.EnumeratedValues;

import java.lang.reflect.Field;

/**
 * <code>RowsetDefinition</code> defines a rowset, including the columns it
 * should contain.
 *
 * <p>See "XML for Analysis Rowsets", page 38 of the XML for Analysis
 * Specification, version 1.1.
 */
class RowsetDefinition {
    final String name;
    final Column[] columnDefinitions;

    RowsetDefinition(String name, Column[] columnDefinitions) {
        this.name = name;
        this.columnDefinitions = columnDefinitions;
    }

    static class Type extends EnumeratedValues.BasicValue {
        public static final int String_ORDINAL = 0;
        public static final Type String = new Type("String", String_ORDINAL);
        public static final int StringArray_ORDINAL = 1;
        public static final Type StringArray = new Type("StringArray", StringArray_ORDINAL);
        public static final int Array_ORDINAL = 2;
        public static final Type Array = new Type("Array", Array_ORDINAL);
        public static final int Enumeration_ORDINAL = 3;
        public static final Type Enumeration = new Type("Enumeration", Enumeration_ORDINAL);
        public static final int EnumerationArray_ORDINAL = 4;
        public static final Type EnumerationArray = new Type("EnumerationArray", EnumerationArray_ORDINAL);
        public static final int EnumString_ORDINAL = 5;
        public static final Type EnumString = new Type("EnumString", EnumString_ORDINAL);
        public static final int Boolean_ORDINAL = 6;
        public static final Type Boolean = new Type("Boolean", Boolean_ORDINAL);
        public static final int StringSometimesArray_ORDINAL = 7;
        public static final Type StringSometimesArray = new Type("StringSometimesArray", StringSometimesArray_ORDINAL);
        public static final int Integer_ORDINAL = 8;
        public static final Type Integer = new Type("Integer", Integer_ORDINAL);
        public static final int UnsignedInteger_ORDINAL = 9;
        public static final Type UnsignedInteger = new Type("UnsignedInteger", UnsignedInteger_ORDINAL);

        public Type(String name, int ordinal) {
            super(name, ordinal, null);
        }

        public static final EnumeratedValues enumeration = new EnumeratedValues(
                new Type[] {
                    String, StringArray, Array, Enumeration, EnumerationArray, EnumString,
                });

        boolean isEnum() {
            switch (ordinal_) {
            case Enumeration_ORDINAL:
            case EnumerationArray_ORDINAL:
            case EnumString_ORDINAL:
                return true;
            }
            return false;
        }
    }

    static class Column {
        final String name;
        final Type type;
        final EnumeratedValues enumeratedType;
        final String description;
        final boolean restriction;
        final boolean nullable;

        /**
         * Creates a column.
         * @param name
         * @param type A {@link Type} value
         * @param enumeratedType Must be specified for enumeration or array
         *   of enumerations
         * @param description
         * @param restriction
         * @param nullable
         * @pre type != null
         * @pre (type == Type.Enumeration || type == Type.EnumerationArray) == (enumeratedType != null)
         */
        Column(String name, Type type, EnumeratedValues enumeratedType,
                boolean restriction, boolean nullable, String description) {
            Util.assertPrecondition(type != null, "Type.instance.isValid(type)");
            Util.assertPrecondition((type == Type.Enumeration || type == Type.EnumerationArray) == (enumeratedType != null), "(type == Type.Enumeration || type == Type.EnumerationArray) == (enumeratedType != null)");
            this.name = name;
            this.type = type;
            this.enumeratedType = enumeratedType;
            this.description = description;
            this.restriction = restriction;
            this.nullable = nullable;
        }

        /**
         * Retrieves a value of this column from a row. The base implementation
         * uses reflection; a derived class may provide a different
         * implementation.
         */
        String get(Object row) {
            try {
                String javaFieldName = name.substring(0, 1).toLowerCase() + name.substring(1);
                Field field = row.getClass().getField(javaFieldName);
                return (String) field.get(row);
            } catch (NoSuchFieldException e) {
                throw Util.newInternal(e, "Error while accessing rowset column " + name);
            } catch (SecurityException e) {
                throw Util.newInternal(e, "Error while accessing rowset column " + name);
            } catch (IllegalAccessException e) {
                throw Util.newInternal(e, "Error while accessing rowset column " + name);
            } catch (ClassCastException e) {
                throw Util.newInternal(e, "Error while accessing rowset column " + name);
            }
        }
    }
}
