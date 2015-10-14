package presto.aws;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * @author haitao.yao
 */
public class AWSConnectorSplit implements ConnectorSplit {

    private final String connectorId;

    private final String schemaName;

    private final String tableName;

    private final boolean remoteAccessible;

    private final AWSConnectorConfig awsConnectorConfig;

    public AWSConnectorSplit(String connectorId, String schemaName, String tableName, AWSConnectorConfig awsConnectorConfig) {
        this.connectorId = checkNotNull(connectorId);
        this.schemaName = checkNotNull(schemaName);
        this.tableName = checkNotNull(tableName);
        this.awsConnectorConfig = checkNotNull(awsConnectorConfig, "awsConnectorConfig should not be null");
        this.remoteAccessible = true;
    }

    public AWSConnectorConfig getAwsConnectorConfig() {
        return this.awsConnectorConfig;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getConnectorId() {
        return connectorId;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return this.remoteAccessible;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo() {
        return this;
    }
}
