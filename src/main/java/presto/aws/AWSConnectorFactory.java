package presto.aws;

import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.type.TypeManager;

import java.util.HashMap;
import java.util.Map;

/**
 * @author haitao.yao
 */
public class AWSConnectorFactory implements ConnectorFactory {

    private final Map<String, String> optionalCofnig;

    private final TypeManager typeManager;

    public AWSConnectorFactory(Map<String, String> optionalConfig, TypeManager typeManager) {
        this.optionalCofnig = optionalConfig;
        this.typeManager = typeManager;
    }

    @Override
    public String getName() {
        return "aws";
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfig) {
        final Map<String, String> rawConfig = new HashMap<String, String>(requiredConfig);
        rawConfig.putAll(this.optionalCofnig);
        final AWSConnectorConfig config = new AWSConnectorConfig(rawConfig);
        return new AWSConnector(connectorId, config);
    }
}
