package presto.aws;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 *
 * @author haitao.yao
 */
public class AWSColumnHandleTest {

    @Test
    public void testToBeanFieldName(){
        final String columnName = "aa_bb_cc_dd";
        final String beanFieldName = AWSColumnHandle.toBeanFieldName(columnName);
        assertEquals("aaBbCcDd", beanFieldName);
    }
}
