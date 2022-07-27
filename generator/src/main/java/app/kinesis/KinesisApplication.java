package app.kinesis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "app")
public class KinesisApplication {

  public static void main(String...args) {
    SpringApplication.run(KinesisApplication.class, args);
  }

}
