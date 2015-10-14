package presto.aws;

import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;


/**
 * entry for Presto AWS plugin
 *
 * @author haitao.yao
 */
public class AWSPlugin implements Plugin {

    private Map<String, String> optionalConfig;
    /**
     * inner type manager from presto
     */
    private TypeManager typeManager;

    @Override
    public void setOptionalConfig(Map<String, String> optionalConfig) {
        this.optionalConfig = Preconditions.checkNotNull(optionalConfig, "optionalConfig should not be null for AWS presto");
    }

    @Inject
    public void setTypeManager(final TypeManager typeManager) {
        this.typeManager = Preconditions.checkNotNull(typeManager, "typeManager should not be null");
    }

    @Override
    public <T> List<T> getServices(Class<T> type) {
        if (type == ConnectorFactory.class) {
            return ImmutableList.of(type.cast(new AWSConnectorFactory(optionalConfig, typeManager)));
        } else {
            return ImmutableList.of();
        }
    }
}
