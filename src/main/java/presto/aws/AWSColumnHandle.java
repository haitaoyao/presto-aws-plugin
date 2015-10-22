package presto.aws;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;

/**
 * @author haitao.yao
 */
public class AWSColumnHandle implements ColumnHandle {

    private final String connectorId;
    private final String columnName;
    private final Type columnType;
    private final String beanFieldName;

    public AWSColumnHandle(String connectorId, ColumnMetadata metadata) {
        this.connectorId = connectorId;
        this.columnName = metadata.getName();
        this.columnType = metadata.getType();
        this.beanFieldName = this.toBeanFieldName(this.columnName);
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

    public Type getColumnType() {
        return columnType;
    }

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

    public String getConnectorId() {
        return connectorId;
    }


}
