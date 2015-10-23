package presto.aws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Preconditions;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.IOUtils;
import static presto.aws.Types.checkType;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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

    private final LoadingCache<SchemaTableName, TableMetaHolder> metaCache = CacheBuilder.newBuilder()
      .build(new CacheLoader<SchemaTableName, TableMetaHolder>() {
          @Override
          public TableMetaHolder load(SchemaTableName key) throws Exception {
              final ConnectorTableMetadata tableMetadata = parseTableMetadata(key);
              if (tableMetadata == null) {
                  return null;
              }
              return new TableMetaHolder(tableMetadata);
          }
      });

    protected static ConnectorTableMetadata parseTableMetadata(SchemaTableName schemaTableName) {
        final String configFileName = "presto/aws/" + schemaTableName.getSchemaName() + "/" + schemaTableName.getTableName() + ".json";
        final InputStream resource = AWSConnectorMetadata.class.getClassLoader().getResourceAsStream(configFileName);
        if (resource == null) {
            return null;
        }
        String content;
        try {
            content = new String(IOUtils.toByteArray(resource));
        } catch (Exception e) {
            throw new IllegalStateException("failed to parse table meta: " + schemaTableName);
        }
        JSONArray columns = JSON.parseArray(content);
        List<ColumnMetadata> columnMetadataList = Lists.newArrayListWithCapacity(columns.size());
        for (Object col : columns) {
            JSONObject value = (JSONObject) col;
            final String columnName = (String) value.get("name");
            final String comment = (String) value.get("comment");
            final String type = (String) value.get("type");
            Type dataType = VarcharType.VARCHAR;
            if ("integer".equals(type)) {
                dataType = BigintType.BIGINT;
            } else if ("datetime".equals(type)) {
                dataType = DateType.DATE;
            } else if ("boolean".equals(type)) {
                dataType = BooleanType.BOOLEAN;
            }
            ColumnMetadata metadata = new ColumnMetadata(columnName, dataType, false, comment, false);
            columnMetadataList.add(metadata);
        }
        final ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(schemaTableName, columnMetadataList);
        return tableMetadata;
    }

    private static class TableMetaHolder {
        final ConnectorTableMetadata tableMetadata;
        final Map<String, ColumnMetadata> columnNameIndex = Maps.newHashMap();

        TableMetaHolder(ConnectorTableMetadata tableMetadata) {
            this.tableMetadata = checkNotNull(tableMetadata);
            for (ColumnMetadata colMetadata : this.tableMetadata.getColumns()) {
                this.columnNameIndex.put(colMetadata.getName(), colMetadata);
            }
        }
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
        return getTableMetadata(tableHandle);
    }

    private ConnectorTableMetadata getTableMetadata(AWSTableHandle tableHandle) {
        final TableMetaHolder tableMetaHolder;
        try {
            tableMetaHolder = this.metaCache.get(tableHandle.toSchemaTableName());
        } catch (ExecutionException e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "failed to get meta data for table: " + tableHandle, e);

        }
        if (tableMetaHolder != null) {
            return tableMetaHolder.tableMetadata;
        } else {
            return null;
        }
    }

    private ColumnMetadata getColumnMetadata(AWSTableHandle tableHandle, final String columnName) {
        final TableMetaHolder tableMetaHolder;
        try {
            tableMetaHolder = this.metaCache.get(tableHandle.toSchemaTableName());
        } catch (ExecutionException e) {
            throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "failed to get meta data for table: " + tableHandle, e);

        }
        if (tableMetaHolder != null) {
            return tableMetaHolder.columnNameIndex.get(columnName);
        } else {
            return null;
        }
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
        final ConnectorTableMetadata connectorTableMetadata = this.getTableMetadata(awsTableHandle);
        if (connectorTableMetadata == null) {
            return null;
        }
        Map<String, ColumnHandle> columnHandles = Maps.newHashMap();
        for (ColumnMetadata metadata : connectorTableMetadata.getColumns()) {
            columnHandles.put(metadata.getName(), new AWSColumnHandle(this.connectorId, metadata.getName(), metadata.getType()));
        }
        return columnHandles;
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle) {
        AWSTableHandle awsTableHandle = checkType(tableHandle, AWSTableHandle.class, "not aws table handle");
        ConnectorTableMetadata tableMetadata = this.getTableMetadata(awsTableHandle);
        if (tableMetadata == null) {
            return null;
        }
        AWSColumnHandle awsColumnHandle = checkType(columnHandle, AWSColumnHandle.class, "not AWSColumnHandle");
        return this.getColumnMetadata(awsTableHandle, awsColumnHandle.getColumnName());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        final SchemaTableName tableName = new SchemaTableName(prefix.getSchemaName(), prefix.getTableName());
        TableMetaHolder tableMetaHolder = null;
        try {
            tableMetaHolder = this.metaCache.get(tableName);
        } catch (ExecutionException e) {

        }
        if (tableMetaHolder == null) {
            return Collections.emptyMap();
        }
        final List<ColumnMetadata> columnMetadatas = Lists.newArrayList(tableMetaHolder.columnNameIndex.values());
        Map<SchemaTableName, List<ColumnMetadata>> result = Maps.newHashMap();
        result.put(tableName, columnMetadatas);
        return result;
    }
}

