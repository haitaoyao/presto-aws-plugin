package presto.aws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.net.URL;
import java.util.List;

/**
 * get the table column info from the json config file
 *
 * @author haitao.yao
 */
public class TableInfoManager {

    public List<ColumnMetadata> getInstancesColumns() throws Exception {
        final String configFileName = "presto/aws/instances.json";
        final URL resource = getClass().getClassLoader().getResource(configFileName);
        if (resource == null) {
            throw new IllegalArgumentException("config file not found in classpath, name: " + configFileName);
        }
        final String content = FileUtils.readFileToString(new File(resource.toURI()));
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
            }
            ColumnMetadata metadata = new ColumnMetadata(columnName, dataType, false, comment, false);
            columnMetadataList.add(metadata);
        }
        return columnMetadataList;
    }
}
