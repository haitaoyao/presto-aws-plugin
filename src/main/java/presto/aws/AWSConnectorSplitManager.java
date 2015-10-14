package presto.aws;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import static presto.aws.Types.checkType;

import java.util.List;

/**
 * @author haitao.yao
 */
public class AWSConnectorSplitManager implements ConnectorSplitManager {

    private final String connectorId;

    private final AWSConnectorConfig awsConnectorConfig;

    public AWSConnectorSplitManager(String connectorId, AWSConnectorConfig awsConnectorConfig) {
        this.connectorId = Preconditions.checkNotNull(connectorId);
        this.awsConnectorConfig = Preconditions.checkNotNull(awsConnectorConfig);
    }

    @Override
    public ConnectorPartitionResult getPartitions(ConnectorSession session, ConnectorTableHandle table, TupleDomain<ColumnHandle> tupleDomain) {
        AWSTableHandle awsTableHandle = checkType(table, AWSTableHandle.class, "not AWSTableHandle");
        return new ConnectorPartitionResult(ImmutableList.<ConnectorPartition>of(new AWSConnectorPartition(awsTableHandle, tupleDomain)), tupleDomain);
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorSession session, ConnectorTableHandle table, List<ConnectorPartition> partitions) {
        if (partitions.isEmpty()) {
            return new FixedSplitSource(this.connectorId, ImmutableList.<ConnectorSplit>of());
        }
        Preconditions.checkArgument(partitions.size() == 1, " > 1 partitions found : %s", partitions.size());
        AWSTableHandle awsTableHandle = checkType(table, AWSTableHandle.class, "not aws table");
        AWSConnectorPartition awsPartition = checkType(partitions.get(0), AWSConnectorPartition.class, "not aws partition");
        AWSConnectorSplit split = new AWSConnectorSplit(this.connectorId, awsTableHandle,
          this.awsConnectorConfig, awsPartition.getTupleDomain());
        return new FixedSplitSource(this.connectorId, ImmutableList.of(split));
    }
}
