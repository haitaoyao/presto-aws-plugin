package presto.aws;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author haitao.yao
 */
public class InstanceRecordSet implements RecordSet {

    private final AWSConnectorSplit connectorSplit;

    private final List<AWSColumnHandle> columnHandles;

    private final List<Type> columnTypes;

    private final AmazonEC2 amazonEC2;

    public InstanceRecordSet(AWSConnectorSplit connectorSplit, List<AWSColumnHandle> columnHandles, AmazonEC2 amazonEC2) {
        this.connectorSplit = connectorSplit;
        this.columnHandles = columnHandles;
        this.amazonEC2 = amazonEC2;
        this.columnTypes = this.columnHandles.stream().map((columnHandle) -> columnHandle.getColumnType()).collect(Collectors.toList());
    }

    @Override
    public List<Type> getColumnTypes() {
        return this.columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        final DescribeInstancesResult describeInstancesResult = this.amazonEC2.describeInstances();
        final List<Instance> instances = describeInstancesResult.getReservations()
          .stream().flatMap((r) -> r.getInstances().stream()).collect(Collectors.toList());
        return new InstanceRecordCursor(instances);
    }

    class InstanceRecordCursor implements RecordCursor {

        public InstanceRecordCursor(List<Instance> instances) {
            this.iterator = instances.iterator();
            this.fieldMapping = Types.getClassFieldMapping(Instance.class);
        }

        private Instance currentInstance = null;

        private Map<String, Field> fieldMapping;

        private final Iterator<Instance> iterator;


        @Override
        public long getTotalBytes() {
            return 0;
        }

        @Override
        public long getCompletedBytes() {
            return 0;
        }

        @Override
        public long getReadTimeNanos() {
            return 0;
        }

        @Override
        public Type getType(int field) {
            return columnTypes.get(field);
        }

        @Override
        public boolean advanceNextPosition() {
            boolean hasNext = this.iterator.hasNext();
            if (hasNext) {
                this.currentInstance = iterator.next();
            }
            return hasNext;
        }

        @Override
        public boolean getBoolean(int field) {
            return doGetValue(field, (f) -> {
                try {
                    return f.getBoolean(this.currentInstance);
                } catch (IllegalAccessException e) {
                    throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "failed to get instance field value: " + field);
                }
            });
        }

        private <R> R doGetValue(int field, Function<Field, R> func) {
            final AWSColumnHandle awsColumnHandle = columnHandles.get(field);
            Field classField = this.fieldMapping.get(awsColumnHandle.getBeanFieldName());
            if (classField == null) {
                throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "no field found for instances, name: " + awsColumnHandle.getBeanFieldName());
            }
            return func.apply(classField);
        }

        @Override
        public long getLong(int field) {
            return doGetValue(field, (f) -> {
                try {
                    return f.getLong(this.currentInstance);
                } catch (IllegalAccessException e) {
                    throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "failed to get instance field value: " + field);
                }
            });
        }

        @Override
        public double getDouble(int field) {
            return doGetValue(field, (f) -> {
                try {
                    return f.getDouble(this.currentInstance);
                } catch (IllegalAccessException e) {
                    throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "failed to get instance field value: " + field);
                }
            });
        }

        @Override
        public Slice getSlice(int field) {
            return null;
        }

        @Override
        public Object getObject(int field) {
            return doGetValue(field, (f) -> {
                try {
                    return f.get(this.currentInstance);
                } catch (IllegalAccessException e) {
                    throw new PrestoException(StandardErrorCode.INTERNAL_ERROR, "failed to get instance field value: " + field);
                }
            });
        }

        @Override
        public boolean isNull(int field) {
            return this.getObject(field) == null;
        }

        @Override
        public void close() {

        }
    }
}
