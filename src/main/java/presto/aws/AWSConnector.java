package presto.aws;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.google.common.base.Preconditions;

/**
 * @author haitao.yao
 */
public class AWSConnector implements Connector {

    private final String connectorId;

    private final AWSConnectorConfig awsConnectorConfig;

    public AWSConnector(final String connectorId, final AWSConnectorConfig awsConnectorConfig) {
        this.connectorId = Preconditions.checkNotNull(connectorId, "connectorId should not be null");
        this.awsConnectorConfig = Preconditions.checkNotNull(awsConnectorConfig, "awsConnectorConfig should not be null");
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new AWSConnectorHandleResolver(this.connectorId);
    }

    @Override
    public ConnectorMetadata getMetadata() {
        return new AWSConnectorMetadata(this.connectorId);
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return new AWSConnectorSplitManager(this.connectorId, this.awsConnectorConfig);
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return new AWSConnectorRecordSetProvider();
    }
}
