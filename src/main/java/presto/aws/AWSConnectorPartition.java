package presto.aws;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Preconditions;

/**
 * @author haitao.yao
 */
public class AWSConnectorPartition implements ConnectorPartition {

    private final AWSTableHandle tableHandle;

    private final TupleDomain<ColumnHandle> tupleDomain;

    public AWSConnectorPartition(AWSTableHandle tableHandle, TupleDomain<ColumnHandle> tupleDomain) {
        this.tableHandle = Preconditions.checkNotNull(tableHandle);
        this.tupleDomain = Preconditions.checkNotNull(tupleDomain);
    }

    @Override
    public String getPartitionId() {
        return this.tableHandle.toString();
    }

    @Override
    public TupleDomain<ColumnHandle> getTupleDomain() {
        return this.tupleDomain;
    }
}
