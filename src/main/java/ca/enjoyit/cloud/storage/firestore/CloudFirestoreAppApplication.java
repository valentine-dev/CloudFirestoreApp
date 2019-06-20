package ca.enjoyit.cloud.storage.firestore;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CloudFirestoreAppApplication {

	public static void main(String[] args) {
		SpringApplication.run(CloudFirestoreAppApplication.class, args);
		
		System.setProperty("https.proxyHost", "your proxy host IP");
		System.setProperty("https.proxyPort", "8080");
		System.setProperty("com.google.api.client.should_use_proxy", "true");
	}

}
