package rptu.thesis.npham.dsserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@SpringBootApplication(scanBasePackages = "rptu.thesis.npham")
@EnableMongoRepositories(basePackages = "rptu.thesis.npham.dsserver.repository")
public class DSServerApplication {

	public static void main(String[] args) {
		SpringApplication.run(DSServerApplication.class, args);
	}
}
