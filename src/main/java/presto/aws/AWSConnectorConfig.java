package presto.aws;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import java.util.Map;

/**
 * @author haitao.yao
 */
public class AWSConnectorConfig {

    private String region;

    private String accessKeyId;

    private String secretAccessKey;

    @JSONCreator
    public AWSConnectorConfig(
      @JsonProperty("region")
      String region,
      @JsonProperty("accessKeyId")
      String accessKeyId,
      @JsonProperty("secretAccessKey")
      String secretAccessKey
    ) {
        this.region = region;
        this.accessKeyId = accessKeyId;
        this.secretAccessKey = secretAccessKey;
    }

    public AWSConnectorConfig(Map<String, String> config) {
        Preconditions.checkNotNull(config);
        this.region = config.get("region");
        this.accessKeyId = config.get("access-key-id");
        this.secretAccessKey = config.get("secret-access-key");
    }

    @JsonProperty
    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    @JsonProperty
    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    @JsonProperty
    public String getSecretAccessKey() {
        return secretAccessKey;
    }

    public void setSecretAccessKey(String secretAccessKey) {
        this.secretAccessKey = secretAccessKey;
    }
}
