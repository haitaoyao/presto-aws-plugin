package presto.aws;

import com.alibaba.fastjson.annotation.JSONCreator;
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
    
    public AmazonEC2 createEC2Client(){
        AWSCredentialsProvider chain;
        if (this.getAccessKeyId() != null && this.getSecretAccessKey() != null) {
            chain = new AWSCredentialsProviderChain(new StaticCredentialsProvider(new BasicAWSCredentials(this.getAccessKeyId(), this.getSecretAccessKey())));
        } else {
            chain = new AWSCredentialsProviderChain(new EnvironmentVariableCredentialsProvider(), new InstanceProfileCredentialsProvider());
        }
        AmazonEC2 client = new AmazonEC2Client(chain);
        Region region;
        if (this.getRegion() != null) {
            region = Region.getRegion(Regions.fromName(this.getRegion()));
        } else {
            region = Regions.getCurrentRegion();
        }
        client.setRegion(region);
        return client;
    }
}
