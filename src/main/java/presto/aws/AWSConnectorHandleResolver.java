package presto.aws;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;

/**
 * @author haitao.yao
 */
public class AWSConnectorHandleResolver implements ConnectorHandleResolver {

    public AWSConnectorHandleResolver(String connectorId) {
        this.connectorId = connectorId;
    }

    private final String connectorId;

    @Override
    public boolean canHandle(ConnectorTableHandle tableHandle) {
        if (!(tableHandle instanceof AWSTableHandle)) {
            return false;
        }
        return this.connectorId.equals(AWSTableHandle.class.cast(tableHandle).getConnectorId());
    }

    @Override
    public boolean canHandle(ColumnHandle columnHandle) {
        if (!(columnHandle instanceof AWSColumnHandle)) {
            return false;
        }
        return this.connectorId.equals(AWSColumnHandle.class.cast(columnHandle).getConnectorId());
    }

    @Override
    public boolean canHandle(ConnectorSplit split) {
        return split instanceof AWSConnectorSplit &&
          this.connectorId.equals(((AWSConnectorSplit) split).getConnectorId());
    }

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return AWSTableHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return AWSColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return AWSConnectorSplit.class;
    }
}
