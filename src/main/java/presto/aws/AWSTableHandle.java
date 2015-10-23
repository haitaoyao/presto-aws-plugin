package presto.aws;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author haitao.yao
 */
public class AWSTableHandle implements ConnectorTableHandle {

    private final String connectorId;

    private final String schemaName;

    private final String tableName;

    @JsonCreator
    public AWSTableHandle(@JsonProperty("connectorId") String connectorId,
                          @JsonProperty("schemaName")
                          String schemaName,
                          @JsonProperty("tableName")
                          String tableName) {
        this.connectorId = connectorId;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AWSTableHandle)) return false;

        AWSTableHandle that = (AWSTableHandle) o;

        if (connectorId != null ? !connectorId.equals(that.connectorId) : that.connectorId != null) return false;
        if (schemaName != null ? !schemaName.equals(that.schemaName) : that.schemaName != null) return false;
        if (tableName != null ? !tableName.equals(that.tableName) : that.tableName != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = connectorId != null ? connectorId.hashCode() : 0;
        result = 31 * result + (schemaName != null ? schemaName.hashCode() : 0);
        result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
        return result;
    }

    public SchemaTableName toSchemaTableName() {
        return new SchemaTableName(this.schemaName, this.tableName);
    }
}
