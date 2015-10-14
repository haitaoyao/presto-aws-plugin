package presto.aws;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import static presto.aws.Types.checkType;

import java.util.List;

/**
 * @author haitao.yao
 */
public class AWSConnectorRecordSetProvider implements ConnectorRecordSetProvider {

    @Override
    public RecordSet getRecordSet(ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
        AWSConnectorSplit awsSplit = checkType(split, AWSConnectorSplit.class, "not AWSConnectorSplit");
        
        return null;
    }
}
