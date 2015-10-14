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
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.amazonaws.util.json.JSONUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Created on 2015-09-25 下午12:03.
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
}
