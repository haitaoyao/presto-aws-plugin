package presto.aws;

import com.facebook.presto.spi.ColumnMetadata;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;

import java.util.List;

/**
 * @author haitao.yao.
 */
public class TableInfoManagerTest {

    @Test
    public void testGetInstanceMeta() throws Exception{
        TableInfoManager manager = new TableInfoManager();
        final List<ColumnMetadata> instancesColumns = manager.getInstancesColumns();
        assertNotNull(instancesColumns);
        assertFalse(instancesColumns.isEmpty());
    }
}
