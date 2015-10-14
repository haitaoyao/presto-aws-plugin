package presto.aws;

import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.ReservedInstances;
import org.junit.Test;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;

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
}
