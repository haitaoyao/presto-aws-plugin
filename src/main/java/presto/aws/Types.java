package presto.aws;

import com.google.common.base.Preconditions;

/**
 * @author haitao.yao
 */
public class Types {

    public static <A, B extends A> B checkType(A value, Class<B> type, String message) {
        Preconditions.checkNotNull(value);
        Preconditions.checkNotNull(type);
        Preconditions.checkArgument(type.isInstance(value), message);
        return type.cast(value);
    }
}
