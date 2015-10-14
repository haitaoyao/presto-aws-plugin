package presto.aws;

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.amazonaws.util.json.JSONUtils;
import com.google.common.collect.ImmutableList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.Test;

/**
 * @author haitao.yao
 */
public class AWSTest {

    private AmazonEC2Client client;

    @Before
    public void setUp(){
        AWSCredentialsProviderChain chain = new AWSCredentialsProviderChain(new EnvironmentVariableCredentialsProvider(), new InstanceProfileCredentialsProvider());
        client = new AmazonEC2Client(chain);
        client.setRegion(Region.getRegion(Regions.CN_NORTH_1));
    }

    @Test
    public void testGetInstance(){
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        final DescribeInstancesResult instancesResult = client.describeInstances(request);
        for(Reservation r : instancesResult.getReservations()){
            System.out.print(r);
        }
    }

    @Test
    public void testGetSecurityGroups(){
        final DescribeSecurityGroupsResult describeSecurityGroupsResult = client.describeSecurityGroups();
        for (SecurityGroup sg : describeSecurityGroupsResult.getSecurityGroups()){
            System.out.println(sg);
        }
    }

    @Test
    public void testFilter(){
        DescribeInstancesRequest request = new DescribeInstancesRequest();
        final Filter filter = new Filter();
        filter.withName("private-ip-address").withValues("172.31.8.162");
        request.withFilters(filter);
        final DescribeInstancesResult instancesResult = this.client.describeInstances(request);
        assertNotNull(instancesResult);
        assertFalse(instancesResult.getReservations().isEmpty());
        int count = 0;
        for(Reservation r : instancesResult.getReservations()){
            for (Instance instance : r.getInstances()){
                System.out.printf("id: %s, private ip: %s\n", instance.getInstanceId(), instance.getPrivateIpAddress());
                count ++;
            }
        }
        System.out.println("total count: " + count);
    }
}
