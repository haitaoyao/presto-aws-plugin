package presto.aws;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.TupleDomain;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * @author haitao.yao
 */
public class AWSConnectorSplit implements ConnectorSplit {

    private final String connectorId;

    private final AWSTableHandle awsTableHandle;

    private final boolean remoteAccessible;

    private final AWSConnectorConfig awsConnectorConfig;

    private final TupleDomain tupleDomain;

    public AWSConnectorSplit(String connectorId, AWSTableHandle awsTableHandle,
                             AWSConnectorConfig awsConnectorConfig,
                             TupleDomain tupleDomain) {
        this.connectorId = checkNotNull(connectorId);
        this.awsTableHandle = checkNotNull(awsTableHandle);
        this.awsConnectorConfig = checkNotNull(awsConnectorConfig, "awsConnectorConfig should not be null");
        this.remoteAccessible = true;
        this.tupleDomain = checkNotNull(tupleDomain, "tupleDomain");
    }

    public AWSConnectorConfig getAwsConnectorConfig() {
        return this.awsConnectorConfig;
    }

    public AWSTableHandle getTableHandle() {
        return this.awsTableHandle;
    }

    public AWSTableHandle getAwsTableHandle() {
        return awsTableHandle;
    }

    public TupleDomain getTupleDomain(){
        return this.tupleDomain;
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
