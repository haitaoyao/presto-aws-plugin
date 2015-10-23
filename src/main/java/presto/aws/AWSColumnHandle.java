package presto.aws;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author haitao.yao
 */
public class AWSColumnHandle implements ColumnHandle {

    private final String connectorId;
    private final String columnName;
    private final Type columnType;
    private final String beanFieldName;

    @JSONCreator
    public AWSColumnHandle(
      @JsonProperty("connectorId")
      String connectorId,
      @JsonProperty("columnName")
      String columnName,
      @JsonProperty("columnType")
      Type columnType) {
        this.connectorId = connectorId;
        this.columnName = columnName;
        this.columnType = columnType;
        this.beanFieldName = toBeanFieldName(this.columnName);
    }

    public static String toBeanFieldName(String tableColumnName) {
        StringBuilder beanFieldName = new StringBuilder(tableColumnName.length());
        boolean startUpper = false;
        for (char c : tableColumnName.toCharArray()) {
            if (c == '_') {
                startUpper = true;
            } else if (startUpper) {
                beanFieldName.append((char) (c - 32));
                startUpper = false;
            } else {
                beanFieldName.append(c);
            }
        }
        return beanFieldName.toString();
    }

    public String getBeanFieldName() {
        return this.beanFieldName;
    }

    @JsonProperty
    public Type getColumnType() {
        return columnType;
    }

    @JsonProperty
    public String getColumnName() {
        return columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AWSColumnHandle)) return false;

        AWSColumnHandle that = (AWSColumnHandle) o;

        if (columnName != null ? !columnName.equals(that.columnName) : that.columnName != null) return false;
        if (columnType != null ? !columnType.equals(that.columnType) : that.columnType != null) return false;
        if (connectorId != null ? !connectorId.equals(that.connectorId) : that.connectorId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = connectorId != null ? connectorId.hashCode() : 0;
        result = 31 * result + (columnName != null ? columnName.hashCode() : 0);
        result = 31 * result + (columnType != null ? columnType.hashCode() : 0);
        return result;
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }


}
