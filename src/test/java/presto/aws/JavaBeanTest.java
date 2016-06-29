package presto.aws;

import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.ReservedInstances;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.mvel2.MVEL;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

/**
 * @author haitao.yao
 */
public class JavaBeanTest {

    @Test
    public void testDescribeInstance() throws Exception{
        final BeanInfo beanInfo = Introspector.getBeanInfo(Instance.class);
        for(PropertyDescriptor bd : beanInfo.getPropertyDescriptors()){
            System.out.println(bd.getName());
        }
    }
    @Test
    public void testFieldSequence(){
        final Field[] declaredFields = Instance.class.getDeclaredFields();
        assertEquals("instanceId", declaredFields[0].getName());
    }

    @Test
    public void testMVEL(){
        final Instance instance = new Instance();
        final String targetInstanceId = "test value";
        instance.setInstanceId(targetInstanceId);
        final Object instanceId = MVEL.eval("instanceId", instance);
        assertEquals(targetInstanceId, instanceId);
    }
}
