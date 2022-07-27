package app.kinesis;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class KinesisGenerator {

  public static final String STREAM_NAME = KinesisConifg.STREAM_NAME;

  public static final String ACCESS_KEY = KinesisConifg.ACCESS_KEY;
  public static final String SECRET_KEY = KinesisConifg.SECRET_KEY;
  public static final String AWS_REGION = KinesisConifg.AWS_REGION;


  public static void main(String... args) throws Exception {

    BasicAWSCredentials awsCredentials = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);

    AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
    clientBuilder.setRegion(AWS_REGION);
    clientBuilder.setCredentials(new AWSStaticCredentialsProvider(awsCredentials));

    AmazonKinesis kinesisClient = clientBuilder.build();

    generate(kinesisClient);

  }

  public static void generate(AmazonKinesis kinesisClient) throws Exception {
    while (true) {

      sendDate(kinesisClient);

      try {
        Thread.sleep(1000);
      } catch (InterruptedException exception) {
        throw new RuntimeException(exception);
      }
    }

  }

  public static void sendDate(AmazonKinesis kinesisClient) throws Exception {

    PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
    putRecordsRequest.setStreamName(STREAM_NAME);
    List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
      String myData = createData();
      putRecordsRequestEntry.setData(ByteBuffer.wrap(myData.getBytes()));
      putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", i / 5));
      putRecordsRequestEntryList.add(putRecordsRequestEntry);
    }
    putRecordsRequest.setRecords(putRecordsRequestEntryList);
    PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
    System.out.println("Put Result" + putRecordsResult);

  }

  public static String createData() throws JsonProcessingException {
    List<String> d1List = ImmutableList.of("d1-a", "d1-b", "d1-c");
    List<String> d2List = ImmutableList.of("d2-x", "d2-y", "d2-z");

    String eventTime = createCurrentTimeString();
    String d1 = d1List.get(new Random().nextInt(100) % 3);
    String d2 = d2List.get(new Random().nextInt(100) % 3);

    double m1 = new Random().nextDouble() * 10;
    double m2 = new Random().nextDouble() * 25;

    Map<String, Object> map = new HashMap<>();
    map.put("event_time", eventTime);
    map.put("d1", d1);
    map.put("d2", d2);
    map.put("m1", m1);
    map.put("m2", m2);

    ObjectMapper objectMapper = new ObjectMapper();

    String jsonString = objectMapper.writeValueAsString(map);

    return jsonString;
  }

  public static String createCurrentTimeString() {
    String pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSX";
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
    Instant instant = Instant.now();
    ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));
    String timeString = formatter.format(zonedDateTime);
    return timeString;
  }

}
