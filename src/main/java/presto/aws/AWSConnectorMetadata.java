package presto.aws;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import static presto.aws.Types.checkType;

import java.util.List;
import java.util.Map;

/**
 * get the meta from AWS
 *
 * @author haitao.yao
 */
public class AWSConnectorMetadata implements ConnectorMetadata {

    public static final String EC2_SCHEMA_NAME = "ec2";

    public static final String EC2_INSTANCES_TABLE_NAME = "instances";

    public static final String EC2_SECURITY_GROUPS_TABLE_NAME = "security_groups";

    private final String connectorId;

    private static final Map<SchemaTableName, ConnectorTableMetadata> META_REGISTRY = Maps.newHashMap();

    static {
        final List<ColumnMetadata> instanceColumnMetas = ImmutableList.of(new ColumnMetadata("instance_id", VarcharType.VARCHAR, false),
          new ColumnMetadata("name", VarcharType.VARCHAR, false));
        ConnectorTableMetadata ec2Table = new ConnectorTableMetadata(new SchemaTableName(EC2_SCHEMA_NAME, EC2_INSTANCES_TABLE_NAME),
          instanceColumnMetas);
        META_REGISTRY.put(ec2Table.getTable(), ec2Table);

        final List<ColumnMetadata> columnMetadatas = ImmutableList.of(new ColumnMetadata("group_id", VarcharType.VARCHAR, false),
          new ColumnMetadata("name", VarcharType.VARCHAR, false));
        ConnectorTableMetadata securityGroupTableMetadata = new ConnectorTableMetadata(
          new SchemaTableName(EC2_SCHEMA_NAME, EC2_SECURITY_GROUPS_TABLE_NAME), columnMetadatas);
        META_REGISTRY.put(securityGroupTableMetadata.getTable(), securityGroupTableMetadata);
    }

    public AWSConnectorMetadata(final String connectorId) {
        this.connectorId = Preconditions.checkNotNull(connectorId, "connectorId should not be null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return ImmutableList.of(EC2_SCHEMA_NAME);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        if (!this.listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }
        return new AWSTableHandle(this.connectorId, tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        AWSTableHandle tableHandle = checkType(table, AWSTableHandle.class, "not AWSTableHandle");
        return META_REGISTRY.get(tableHandle.toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, String schemaNameOrNull) {
        if ("ec2".equals(schemaNameOrNull)) {
            return ImmutableList.of(new SchemaTableName(schemaNameOrNull, EC2_INSTANCES_TABLE_NAME),
              new SchemaTableName(schemaNameOrNull, EC2_SECURITY_GROUPS_TABLE_NAME));
        }
        return null;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        AWSTableHandle awsTableHandle = checkType(tableHandle, AWSTableHandle.class, "not aws table handle");
        final ConnectorTableMetadata connectorTableMetadata = META_REGISTRY.get(awsTableHandle.toSchemaTableName());
        if(connectorTableMetadata == null){
            return null;
        }
        Map<String, ColumnHandle> columnHandles = Maps.newHashMap();
        for(ColumnMetadata metadata : connectorTableMetadata.getColumns()){
            columnHandles.put(metadata.getName(), new AWSColumnHandle(this.connectorId, metadata));
        }
        return columnHandles;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        AWSTableHandle awsTableHandle = checkType(tableHandle, AWSTableHandle.class, "not aws table handle");
        ConnectorTableMetadata tableMetadata = META_REGISTRY.get(awsTableHandle.toSchemaTableName());
        if(tableMetadata == null){
            return null;
        }
        AWSColumnHandle awsColumnHandle = checkType(columnHandle, AWSColumnHandle.class, "not AWSColumnHandle");
        ColumnMetadata columnMetadata = null;
        for(ColumnMetadata cd : tableMetadata.getColumns()){
            if(cd.getName().equals(awsColumnHandle.getColumnName())){
                columnMetadata = cd;
                break;
            }
        }
        return columnMetadata;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        return null;
    }
}
