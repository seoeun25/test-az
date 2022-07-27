package app.kinesis;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;

import java.util.ArrayList;
import java.util.List;

public class KinesisConsumer {

  public static final String STREAM_NAME = KinesisConifg.STREAM_NAME;

  public static final String ACCESS_KEY = KinesisConifg.ACCESS_KEY;
  public static final String SECRET_KEY = KinesisConifg.SECRET_KEY;
  public static final String AWS_REGION = KinesisConifg.AWS_REGION;


  public static void main(String... args) {
    BasicAWSCredentials awsCredentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);
    AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
    clientBuilder.setRegion(AWS_REGION);
    clientBuilder.setCredentials(new AWSStaticCredentialsProvider(awsCredentials));
    AmazonKinesis kinesisClient = clientBuilder.build();

    List<Shard> shards = getShardInfo(kinesisClient);

    String shard = shards.size() == 0 ? "shardId-000000000000" : shards.get(0).getShardId();
    consume(kinesisClient, shard);


  }

  public static void consume(AmazonKinesis kinesisClient, String shardId) {

    System.out.println("shardId = " + shardId);
    String shardIterator;
    GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
    getShardIteratorRequest.setStreamName(STREAM_NAME);
    getShardIteratorRequest.setShardId(shardId);
    getShardIteratorRequest.setShardIteratorType("LATEST"); // LATEST
    //getShardIteratorRequest.setStartingSequenceNumber("49611861803471740882407752157147120358654448853627961346");

    GetShardIteratorResult getShardIteratorResult = kinesisClient.getShardIterator(getShardIteratorRequest);
    shardIterator = getShardIteratorResult.getShardIterator();
    System.out.println("shardIterator = " + shardIterator);

    // Continuously read data records from a shard
    List<Record> records;
    while (true) {

      // Create a new getRecordsRequest with an existing shardIterator
      // Set the maximum records to return to 25

      GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
      getRecordsRequest.setShardIterator(shardIterator);
      //getRecordsRequest.setLimit(1000);
      System.out.println("shardIterator = " + shardIterator);

      GetRecordsResult result = kinesisClient.getRecords(getRecordsRequest);

      // Put the result into record list. The result can be empty.
      records = result.getRecords();
      System.out.println("records = " + records.size());

      for (Record r : records) {
        System.out.println(r.getSequenceNumber() + ", " + r.getPartitionKey());
        byte[] bytes = r.getData().array();
        System.out.println(new String(bytes));
      }

      try {
        Thread.sleep(1000);
      }
      catch (InterruptedException exception) {
        throw new RuntimeException(exception);
      }

      shardIterator = result.getNextShardIterator();
    }



  }

  public static List<Shard> getShardInfo(AmazonKinesis kinesisClient) {
    DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
    describeStreamRequest.setStreamName( STREAM_NAME );
    List<Shard> shards = new ArrayList<>();
    String exclusiveStartShardId = null;
    do {
      describeStreamRequest.setExclusiveStartShardId( exclusiveStartShardId );
      DescribeStreamResult describeStreamResult = kinesisClient.describeStream( describeStreamRequest );
      shards.addAll( describeStreamResult.getStreamDescription().getShards() );
      if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
        exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
      } else {
        exclusiveStartShardId = null;
      }
    } while ( exclusiveStartShardId != null );

    System.out.println("shards = " + shards.size());
    for (Shard shard: shards) {
      System.out.println("shard : " +shard.getShardId());
      System.out.println(shard);
      System.out.println("parentShard = " + shard.getSequenceNumberRange());
    }
    return shards;
  }

}
