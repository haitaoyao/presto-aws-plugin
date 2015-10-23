package presto.aws;

import com.amazonaws.services.ec2.model.Instance;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.Maps;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * @author haitao.yao
 */
public class AWSConnectorMetadataTest {

    @Test
    public void testParseMeta(){
        final SchemaTableName instances = new SchemaTableName("ec2", "instances");
        final ConnectorTableMetadata tableMetadata = AWSConnectorMetadata.parseTableMetadata(instances);

        assertNotNull(tableMetadata);
        final Map<String, Field> fieldMap = Maps.newHashMap();
        for(Field f : Instance.class.getDeclaredFields()){
            fieldMap.put(f.getName(), f);
        }
        for(ColumnMetadata cmeta : tableMetadata.getColumns()){
            assertNotNull(cmeta.getName() + " not exists", fieldMap.get(AWSColumnHandle.toBeanFieldName(cmeta.getName())));
        }
    }
}
