package presto.aws;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.ConnectorPartitionResult;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.TupleDomain;

import java.util.List;

/**
 * @author haitao.yao
 */
public class AWSConnectorSplitManager implements ConnectorSplitManager {


    @Override
    public ConnectorPartitionResult getPartitions(ConnectorSession session, ConnectorTableHandle table, TupleDomain<ColumnHandle> tupleDomain) {
        return null;
    }

    @Override
    public ConnectorSplitSource getPartitionSplits(ConnectorSession session, ConnectorTableHandle table, List<ConnectorPartition> partitions) {
        return null;
    }
}
