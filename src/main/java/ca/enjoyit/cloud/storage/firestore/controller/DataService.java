/**
 * 
 */
package ca.enjoyit.cloud.storage.firestore.controller;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.FieldValue;
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
	private static final int QUERY_PARAMETER_MAX_SIZE = 3;

	@GetMapping("/")
	public String defaultGreeting() {
		log.debug("Default greeting");

		return "greeting";
	}

	@PostMapping("/user")
	public Object addUser(@RequestBody User user) throws IOException, InterruptedException, ExecutionException {
		log.debug("Adding Data to Cloud Firestore");

		if (user.getId() == null || user.getId().trim().length() == 0) {
			log.error("User ID cannot be blank!");
			return "Invalid Data.";
		}

		user.setTimestamp(Timestamp.now().toString());
		// Use a service account
		InputStream serviceAccount = new FileInputStream(
				"C:\\dev\\projects\\lca\\batch\\\\gcp_service_account\\Users-e0a6db6e4593.json");
		// "C:\\dev\\projects\\firestore\\service_account\\Users-c44a9454da63.json");
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

		return user;
	}

	@PutMapping("/user")
	public Object updateUser(@RequestBody User user) throws IOException, InterruptedException, ExecutionException,
			IllegalArgumentException, IllegalAccessException {
		log.debug("Updating Data to Cloud Firestore");

		if (user.getId() == null || user.getId().trim().length() == 0) {
			log.error("User ID cannot be blank!");
			return "Invalid Data.";
		}

		Map<String, Object> updates = new HashMap<>();
		for (Field field : user.getClass().getDeclaredFields()) {
			field.setAccessible(true);
			System.out.println(field.getName() + " - " + field.getType() + " - " + field.get(user));
			if ((String.class.isAssignableFrom(field.getType()) && field.get(user) != null && !field.getName().equals("id"))
					|| (int.class.equals(field.getType()) && (Integer) field.get(user) != 0)) {
				System.out.println(field.getName() + " will be updated");
				updates.put(field.getName(), field.get(user));
			}
		}
		updates.put("timestamp", Timestamp.now().toString());

		// Use a service account
		InputStream serviceAccount = new FileInputStream(
				"C:\\dev\\projects\\lca\\batch\\\\gcp_service_account\\Users-e0a6db6e4593.json");
		// "C:\\dev\\projects\\firestore\\service_account\\Users-c44a9454da63.json");
		GoogleCredentials credentials = GoogleCredentials.fromStream(serviceAccount);
		FirebaseOptions options = new FirebaseOptions.Builder().setCredentials(credentials).build();
		if (FirebaseApp.getApps().isEmpty()) {
			FirebaseApp.initializeApp(options);
		}

		Firestore db = FirestoreClient.getFirestore();
		DocumentReference docRef = db.collection("users").document(user.getId());

		// asynchronously write data
		ApiFuture<WriteResult> result = docRef.update(updates);
		// ...
		// result.get() blocks on response
		System.out.println("Update time : " + result.get().getUpdateTime());

		return docRef.get().get().toObject(User.class);
	}

	@GetMapping("/user")
	public Object getUser(@RequestParam Map<String, String> queryParams)
			throws IOException, InterruptedException, ExecutionException {
		log.debug("Getting Data from Cloud Firestore...");

		// if no query parameters, queryParams will NOT be null but its size is 0
		if (queryParams == null) {
			log.debug("No parameters ...");
		} else {
			log.debug("No. of query parameters: " + queryParams.size());
		}

		if (queryParams.size() > QUERY_PARAMETER_MAX_SIZE) {
			return "Too many query parameters - maximum: " + QUERY_PARAMETER_MAX_SIZE;
		}

		// Use a service account
		InputStream serviceAccount = new FileInputStream(
				"C:\\dev\\projects\\lca\\batch\\\\gcp_service_account\\Users-e0a6db6e4593.json");
		// "C:\\dev\\projects\\firestore\\service_account\\Users-c44a9454da63.json");
		GoogleCredentials credentials = GoogleCredentials.fromStream(serviceAccount);
		FirebaseOptions options = new FirebaseOptions.Builder().setCredentials(credentials).build();
		if (FirebaseApp.getApps().isEmpty()) {
			FirebaseApp.initializeApp(options);
		}

		Firestore db = FirestoreClient.getFirestore();
		ApiFuture<QuerySnapshot> query = null;

		List<String> keys = new ArrayList<>();
		List<Object> values = new ArrayList<>();

		if (queryParams.size() > 0) {
			queryParams.forEach((k, v) -> {
				if (!keys.contains(k)) {
					keys.add(k);
					if (v == null || v.isEmpty()) {
						values.add(v);
					} else if (v.matches("[-+]?\\d+")) {
						values.add(Integer.parseInt(v));
					} else if (v.matches("[-+]?\\d+\\.\\d*")) {
						values.add(Double.parseDouble(v));
					} else {
						values.add(v);
					}
				}
			});
		}

		switch (keys.size()) {
		case 0:
			query = db.collection("users").get();
			break;
		case 1:
			query = db.collection("users").whereEqualTo(keys.get(0), values.get(0)).get();
			break;
		case 2:
			query = db.collection("users").whereEqualTo(keys.get(0), values.get(0))
					.whereEqualTo(keys.get(1), values.get(1)).get();
			break;
		case 3:
			query = db.collection("users").whereEqualTo(keys.get(0), values.get(0))
					.whereEqualTo(keys.get(1), values.get(1)).whereEqualTo(keys.get(2), values.get(2)).get();
			break;
		default:
			return "Keys size is invalid.";
		}

		// ...
		// query.get() blocks on response
		QuerySnapshot querySnapshot = query.get();
		List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();

		List<User> users = new ArrayList<>();
		for (QueryDocumentSnapshot document : documents) {
			users.add(document.toObject(User.class));
		}

		log.debug("Data read from Cloud Firestore in Java structure: "
				+ new GsonBuilder().setPrettyPrinting().create().toJson(users));

		return users;
	}

	@GetMapping("/user/{id}")
	public Object getUserById(@PathVariable String id) throws IOException, InterruptedException, ExecutionException {
		log.debug("Getting Data by ID from Cloud Firestore");

		// Use a service account
		InputStream serviceAccount = new FileInputStream(
				"C:\\dev\\projects\\lca\\batch\\\\gcp_service_account\\Users-e0a6db6e4593.json");
		// "C:\\dev\\projects\\firestore\\service_account\\Users-c44a9454da63.json");
		GoogleCredentials credentials = GoogleCredentials.fromStream(serviceAccount);
		FirebaseOptions options = new FirebaseOptions.Builder().setCredentials(credentials).build();
		if (FirebaseApp.getApps().isEmpty()) {
			FirebaseApp.initializeApp(options);
		}

		Firestore db = FirestoreClient.getFirestore();
		ApiFuture<DocumentSnapshot> getResult = db.collection("users").document(id).get();
		DocumentSnapshot document = getResult.get();
		if (document.exists()) {
			return document.toObject(User.class);
		} else {
			return "No such document!";
		}
	}

	@DeleteMapping("/user/{id}")
	public String deleteUser(@PathVariable String id) throws IOException, InterruptedException, ExecutionException {
		log.debug("Deleting Data from Cloud Firestore");

		// Use a service account
		InputStream serviceAccount = new FileInputStream(
				"C:\\dev\\projects\\lca\\batch\\\\gcp_service_account\\Users-e0a6db6e4593.json");
		// "C:\\dev\\projects\\firestore\\service_account\\Users-c44a9454da63.json");
		GoogleCredentials credentials = GoogleCredentials.fromStream(serviceAccount);
		FirebaseOptions options = new FirebaseOptions.Builder().setCredentials(credentials).build();
		if (FirebaseApp.getApps().isEmpty()) {
			FirebaseApp.initializeApp(options);
		}

		Firestore db = FirestoreClient.getFirestore();
		ApiFuture<WriteResult> deleteResult = db.collection("users").document(id).delete();
		return "Document Deleted Successfully at " + deleteResult.get().getUpdateTime() + ".";

	}

	@SuppressWarnings("serial")
	@DeleteMapping("/user/{doc}/{field}")
	public String deleteUserField(@PathVariable String doc, @PathVariable String field)
			throws IOException, InterruptedException, ExecutionException {
		log.debug("Deleting Data Field from Cloud Firestore");

		// Use a service account
		InputStream serviceAccount = new FileInputStream(
				"C:\\dev\\projects\\lca\\batch\\\\gcp_service_account\\Users-e0a6db6e4593.json");
		// "C:\\dev\\projects\\firestore\\service_account\\Users-c44a9454da63.json");
		GoogleCredentials credentials = GoogleCredentials.fromStream(serviceAccount);
		FirebaseOptions options = new FirebaseOptions.Builder().setCredentials(credentials).build();
		if (FirebaseApp.getApps().isEmpty()) {
			FirebaseApp.initializeApp(options);
		}

		Firestore db = FirestoreClient.getFirestore();
		ApiFuture<WriteResult> updateResult = db.collection("users").document(doc)
				.update(new HashMap<String, Object>() {
					{
						put(field, FieldValue.delete());
						put("timestamp", Timestamp.now().toString());
					}
				});
		return "Field Deleted Successfully at " + updateResult.get().getUpdateTime() + ".";

	}

}
