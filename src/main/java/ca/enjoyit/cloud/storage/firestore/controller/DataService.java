/**
 * 
 */
package ca.enjoyit.cloud.storage.firestore.controller;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.WriteResult;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import com.google.gson.GsonBuilder;

import ca.enjoyit.cloud.storage.firestore.data.User;

/**
 * @author Valentine Wu
 *
 */
@RestController
public class DataService {
	private static final Log log = LogFactory.getLog(DataService.class);

	@GetMapping("/")
	public String defaultGreeting() {
		log.debug("Default greeting");

		return "greeting";
	}

	@PostMapping("/user")
	public String addUser(@RequestBody User user) throws IOException, InterruptedException, ExecutionException {
		log.debug("Adding Data to Cloud Firestore");

		if (user.getId() == null || user.getId().trim().length() == 0) {
			log.error("User ID cannot be blank!");
			return "Invalid Data.";
		}

		// Use a service account
		InputStream serviceAccount = new FileInputStream(
				"C:\\dev\\projects\\firestore\\service_account\\Users-c44a9454da63.json");
		GoogleCredentials credentials = GoogleCredentials.fromStream(serviceAccount);
		FirebaseOptions options = new FirebaseOptions.Builder().setCredentials(credentials).build();
		if (FirebaseApp.getApps().isEmpty()) {
			FirebaseApp.initializeApp(options);
		}

		Firestore db = FirestoreClient.getFirestore();
		DocumentReference docRef = db.collection("users").document(user.getId());
		// asynchronously write data
		ApiFuture<WriteResult> result = docRef.set(user);
		// ...
		// result.get() blocks on response
		System.out.println("Update time : " + result.get().getUpdateTime());

		String output = "Data added to Cloud Firestore: " + new GsonBuilder().setPrettyPrinting().create().toJson(user)
				+ " Check at https://console.firebase.google.com/project/_/database/firestore/data";

		return output;
	}

	@GetMapping("/user")
	public List<User> getUser() throws IOException, InterruptedException, ExecutionException {
		log.debug("Reading Data from Cloud Firestore...");

		System.setProperty("https.proxyHost", "your_proxy_host");
		System.setProperty("https.proxyPort", "8080");
		System.setProperty("com.google.api.client.should_use_proxy", "true");

		// Use a service account
		InputStream serviceAccount = new FileInputStream(
				"C:\\dev\\projects\\firestore\\service_account\\Users-c44a9454da63.json");
		GoogleCredentials credentials = GoogleCredentials.fromStream(serviceAccount);
		FirebaseOptions options = new FirebaseOptions.Builder().setCredentials(credentials).build();
		if (FirebaseApp.getApps().isEmpty()) {
			FirebaseApp.initializeApp(options);
		}

		Firestore db = FirestoreClient.getFirestore();
		ApiFuture<QuerySnapshot> query = db.collection("users").get();
		// ...
		// query.get() blocks on response
		QuerySnapshot querySnapshot = query.get();
		List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();

		List<User> users = new ArrayList<>();
		for (QueryDocumentSnapshot document : documents) {
			users.add(document.toObject(User.class));
		}

		log.debug("Data read from Cloud Firestore: " + new GsonBuilder().setPrettyPrinting().create().toJson(users));

		return users;
	}

}
