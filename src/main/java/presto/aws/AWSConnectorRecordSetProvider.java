package presto.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import static presto.aws.Types.checkType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author haitao.yao
 */
public class AWSConnectorRecordSetProvider implements ConnectorRecordSetProvider {

    private static final Log LOG = LogFactory.getLog(AWSConnectorRecordSetProvider.class);

    @Override
    public RecordSet getRecordSet(ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
        AWSConnectorSplit awsSplit = checkType(split, AWSConnectorSplit.class, "not AWSConnectorSplit");
        List<AWSColumnHandle> awsColumnHandles = columns.stream().map((v) -> {
            AWSColumnHandle newValue = checkType(v, AWSColumnHandle.class, "not AWSColumnHandle");
            return newValue;
        }).collect(Collectors.toList());
        AmazonEC2 client = createAmazonClient(awsSplit);
        return new InstanceRecordSet(awsSplit, awsColumnHandles, client);
    }

    private AmazonEC2 createAmazonClient(AWSConnectorSplit awsSplit) {
        final AWSConnectorConfig config = awsSplit.getAwsConnectorConfig();
        AWSCredentialsProvider chain;
        if (config.getAccessKeyId() != null && config.getSecretAccessKey() != null) {
            chain = new AWSCredentialsProviderChain(new StaticCredentialsProvider(new BasicAWSCredentials(config.getAccessKeyId(), config.getSecretAccessKey())));
        } else {
            chain = new AWSCredentialsProviderChain(new EnvironmentVariableCredentialsProvider(), new InstanceProfileCredentialsProvider());
        }
        AmazonEC2 client = new AmazonEC2Client(chain);
        Region region;
        if (config.getRegion() != null) {
            region = Region.getRegion(Regions.fromName(config.getRegion()));
        } else {
            region = Regions.getCurrentRegion();
        }
        // region is very important for cn-north-1 region.
        LOG.info("amazon client, using region: " + region);
        client.setRegion(region);
        return client;
    }
}
