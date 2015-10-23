package presto.aws;

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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

    @JsonCreator
    public AWSConnectorSplit(
      @JsonProperty("connectorId")
      String connectorId,
      @JsonProperty("awsTableHandle")
      AWSTableHandle awsTableHandle,
      @JsonProperty("awsConnectorConfig")
      AWSConnectorConfig awsConnectorConfig,
      @JsonProperty("tupleDomain")
      TupleDomain tupleDomain) {
        this.connectorId = checkNotNull(connectorId);
        this.awsTableHandle = checkNotNull(awsTableHandle);
        this.awsConnectorConfig = checkNotNull(awsConnectorConfig, "awsConnectorConfig should not be null");
        this.remoteAccessible = true;
        this.tupleDomain = checkNotNull(tupleDomain, "tupleDomain");
    }

    @JsonProperty
    public AWSConnectorConfig getAwsConnectorConfig() {
        return this.awsConnectorConfig;
    }

    @JsonProperty
    public AWSTableHandle getTableHandle() {
        return this.awsTableHandle;
    }

    @JsonProperty
    public AWSTableHandle getAwsTableHandle() {
        return awsTableHandle;
    }

    @JsonProperty
    public TupleDomain getTupleDomain() {
        return this.tupleDomain;
    }

    @JsonProperty
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
