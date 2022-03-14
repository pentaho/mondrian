package mondrian.junit5;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.extension.ExtendWith;

@Inherited
@Target({
ElementType.TYPE,ElementType.METHOD
})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ExtendWith(MondrianRuntimeExtension.class)
public @interface MondrianRuntimeSupport {
    Class<? extends DatabaseHandler> database() default MySQLDatabaseHandler.class;
    Class<? extends DataLoader> dataLoader() default FastFoodmardDataLoader.class;
   
}
