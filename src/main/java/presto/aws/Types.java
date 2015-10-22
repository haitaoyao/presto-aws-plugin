package presto.aws;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.util.Map;

/**
 * @author haitao.yao
 */
public class Types {

    private Types() {
    }

    public static <A, B extends A> B checkType(A value, Class<B> type, String message) {
        Preconditions.checkNotNull(value);
        Preconditions.checkNotNull(type);
        Preconditions.checkArgument(type.isInstance(value), message);
        return type.cast(value);
    }

    public static final Map<String, Field> getClassFieldMapping(Class<?> clazz) {
        Map<String, Field> fieldMap = Maps.newHashMap();
        for (Field f : clazz.getDeclaredFields()) {
            f.setAccessible(true);
            fieldMap.put(f.getName(), f);
        }
        return fieldMap;
    }
}
