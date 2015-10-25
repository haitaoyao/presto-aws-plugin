package presto.aws;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorPageSourceProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;

import java.util.List;

/**
 * @author haitao.yao
 */
public class AWSConnectorPageSourceProvider implements ConnectorPageSourceProvider {

    @Override
    public ConnectorPageSource createPageSource(ConnectorSession session, ConnectorSplit split, List<ColumnHandle> columns) {
        return null;
    }
}
