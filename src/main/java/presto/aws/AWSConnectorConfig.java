package presto.aws;

import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * @author haitao.yao
 */
public class AWSConnectorConfig {

    private String region;

    private String accessKeyId;

    private String secretAccessKey;

    public AWSConnectorConfig() {
    }

    public AWSConnectorConfig(Map<String, String> config) {
        Preconditions.checkNotNull(config);
        this.region = config.get("region");
        this.accessKeyId = config.get("access-key-id");
        this.secretAccessKey = config.get("secret-access-key");
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public void setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
    }
}
