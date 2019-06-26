/**
 * 
 */
package ca.enjoyit.cloud.storage.firestore.controller;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

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
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.FieldValue;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.WriteResult;
import com.google.gson.GsonBuilder;

import ca.enjoyit.cloud.storage.firestore.data.CanadaCalling;
import ca.enjoyit.cloud.storage.firestore.data.LocalCalling;
import ca.enjoyit.cloud.storage.firestore.data.User;

/**
 * @author Valentine Wu
 *
 */
@RestController
public class DataService {
	private static final Log log = LogFactory.getLog(DataService.class);
	private static final int QUERY_PARAMETER_MAX_SIZE = 3;
	final Map<String, String> RATE_CENTER_NAME_MAPPING = new HashMap<>();

	@GetMapping("/")
	public String defaultGreeting() {
		log.debug("Default greeting");
		String lcLookup = System.getProperty("lca.lookup.file.localcalling"); // checked already
		String message = "Greeting: RATE_CENTER_NAME_MAPPING is set!";
		if (RATE_CENTER_NAME_MAPPING.size() > 0) {
			message = "Greeting: RATE_CENTER_NAME_MAPPING is reset!";
			RATE_CENTER_NAME_MAPPING.clear();
		}
		try (Stream<String> stream = Files.lines(Paths.get(lcLookup), StandardCharsets.UTF_8)) {
			stream.skip(1) // first line is the header -- skip
					.map(x -> x.split(",")).forEach(x -> {
						RATE_CENTER_NAME_MAPPING.put(x[0], x[1]);
					});
			log.debug("Number of rate center name mappings: " + RATE_CENTER_NAME_MAPPING.size());
		} catch (IOException e) {
			e.printStackTrace();
			message = "Greeting: rate center name mappings cannot be read";
			log.error(message);
		}
		return message;
	}

	@GetMapping("/addLocalCalling")
	public String addLocalCalling() throws InterruptedException, ExecutionException {
		log.debug("Adding Local Calling");

		final Map<String, LocalCalling> localCallingRecords = new HashMap<>();
		// checked already
		String inputFile = System.getProperty("lca.report.file.localcalling");
		String processedFile = inputFile.substring(0, inputFile.length() - 4) + "_processed.csv";

		backupFile(processedFile);

		StringBuffer firstline = new StringBuffer();
		try (Stream<String> stream = Files.lines(Paths.get(inputFile), StandardCharsets.UTF_8)) {
			stream.limit(1).map(x -> x.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)"))
					.forEach(x -> writeLocalCallingFirstLine(processedFile, firstline, x));
		} catch (IOException e) {
			e.printStackTrace();
		}

		try (Stream<String> stream = Files.lines(Paths.get(inputFile), StandardCharsets.UTF_8);
				BufferedWriter writer = new BufferedWriter(
						new OutputStreamWriter(new FileOutputStream(processedFile), StandardCharsets.UTF_8))) {
			// write line first line
			writer.append(firstline);
			// process record lines
			stream.skip(1) // first line is the header -- skip
					.map(x -> x.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1)) // https://stackabuse.com/regex-splitting-by-character-unless-in-quotes/
					.forEach(x -> {
						// create record
						createLocalCallingRecord(x, localCallingRecords);
						// write to file
						writeLocalCallingRecords(processedFile, writer, x);
					});
		} catch (IOException e) {
			e.printStackTrace();
		}

		log.debug("Total records: " + localCallingRecords.size());

		writeToCloud(localCallingRecords, "localcalling");

		return "Added!";
	}

	private void createLocalCallingRecord(final String[] row, final Map<String, LocalCalling> localcallingRecords) {
		String key = row[5].trim() + "(" + row[6].trim() + "):" + row[11].trim() + "(" + row[12].trim() + ")";
		LocalCalling value = getLocalCallingRecord(row);
		localcallingRecords.put(key, value);
	}

	private void writeLocalCallingRecords(String processedFile, BufferedWriter writer, String[] row) {
		try {
			writer.append(row[0].trim() + ",");
			writer.append(row[1].trim() + ",");
			writer.append(row[2].trim() + ",");
			writer.append(row[3].trim() + ",");
			writer.append(row[4].trim() + ",");
			writer.append(row[5].trim() + ",");
			writer.append(row[6].trim() + ",");
			writer.append(RATE_CENTER_NAME_MAPPING.getOrDefault(row[5].trim() + row[6].trim(), "") + ",");
			writer.append(row[7].trim() + ",");
			writer.append(row[8].trim() + ",");
			writer.append(row[9].trim() + ",");
			writer.append(row[10].trim() + ",");
			writer.append(row[11].trim() + ",");
			writer.append(row[12].trim() + ",");
			writer.append(RATE_CENTER_NAME_MAPPING.getOrDefault(row[11].trim() + row[12].trim(), "") + ",");
			writer.append(row[13].trim() + ",");
			writer.append(row[14].trim() + ",");
			writer.append(row[15].trim() + ",");
			writer.append(row[16].trim());
			writer.newLine();
		} catch (IOException e) {
			e.printStackTrace();
			log.debug("ERROR: cannot write to processed file - " + processedFile);
		}
	}

	private void writeLocalCallingFirstLine(String processedFile, StringBuffer writer, String[] x) {
		writer.append(x[0].trim() + ",");
		writer.append(x[1].trim() + ",");
		writer.append(x[2].trim() + ",");
		writer.append(x[3].trim() + ",");
		writer.append(x[4].trim() + ",");
		writer.append("originating_ratecenter_prov,");
		writer.append(x[6].trim() + ",");
		writer.append("originating_ratecenter,");
		writer.append(x[7].trim() + ",");
		writer.append(x[8].trim() + ",");
		writer.append(x[9].trim() + ",");
		writer.append(x[10].trim() + ",");
		writer.append("terminating_ratecenter_prov,");
		writer.append(x[12].trim() + ",");
		writer.append("terminating_ratecenter,");
		writer.append(x[13].trim() + ",");
		writer.append(x[14].trim() + ",");
		writer.append(x[15].trim() + ",");
		writer.append(x[16].trim());
		writer.append(System.lineSeparator());
	}

	private void backupFile(final String inputFile) {
		Path inputFilePath = Paths.get(inputFile);
		if (Files.exists(inputFilePath)) {
			String message = "File " + inputFile + " exists already.";
			log.debug(message);
			String newFileName = inputFile + "." + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
			try {
				Files.move(inputFilePath, Paths.get(newFileName));
			} catch (IOException e) {
				log.debug("Exception: rename " + inputFile + " to " + newFileName);
				e.printStackTrace();
				return;
			}
			String movedMessage = "File " + inputFile + " is renamed as " + newFileName + ".";
			log.debug(movedMessage);
		}
	}

	private void writeToCloud(Map<String, ? extends Object> records, final String collectionName) {
		FirestoreOptions firestoreOptions = FirestoreOptions.getDefaultInstance().toBuilder().build();

		try (Firestore db = firestoreOptions.getService();) {

			// Convert all Map keys to a List
			List<String> keyList = new ArrayList<>(records.keySet());

			// Convert all Map values to a List
			List<Object> valueList = new ArrayList<>(records.values());
			int batchSize = 480;
			int totalRecords = records.size();
			int addedRecords = 0;
			while (addedRecords < totalRecords) {
				// Get a new write batch
				WriteBatch batch = db.batch();
				int addedInBatch;
				for (addedInBatch = 0; addedInBatch < batchSize
						&& (addedInBatch + addedRecords) < totalRecords; addedInBatch++) {
					DocumentReference docRef = db.collection(collectionName)
							.document(keyList.get(addedInBatch + addedRecords));
					batch.set(docRef, valueList.get(addedInBatch + addedRecords));
				}
				// asynchronously commit the batch
				ApiFuture<List<WriteResult>> future = batch.commit();
				// ...
				// future.get() blocks on batch commit operation

				future.get();
				addedRecords += addedInBatch;
				log.debug("Added records: " + addedRecords);
			}
			db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private LocalCalling getLocalCallingRecord(final String[] row) {
		LocalCalling record = new LocalCalling();

		// new fields
		record.setOriginating_ratecenter(RATE_CENTER_NAME_MAPPING.getOrDefault(row[5].trim() + row[6].trim(), ""));
		record.setTerminating_ratecenter(RATE_CENTER_NAME_MAPPING.getOrDefault(row[11].trim() + row[12].trim(), ""));

		// fields from the report
		record.setPaco_code(row[0]);
		record.setOpco_code(row[1]);
		record.setCapl_code(row[2]);
		record.setRaco_code(row[3]);
		record.setEffective_date(row[4]);
		record.setOriginating_ratecenter_prov(row[5]);
		record.setOrrg_race_short_name(row[6]);
		record.setOrrg_race_type_code(row[7]);
		record.setOrrg_premium_flag(row[8]);
		record.setOrrg_dist_code(row[9]);
		record.setOrrg_exrc_code(row[10]);
		record.setTerminating_ratecenter_prov(row[11]);
		record.setTerg_race_short_name(row[12]);
		record.setTerg_race_type_code(row[13]);
		record.setTerg_premium_flag(row[14]);
		record.setTerg_dist_code(row[15]);
		record.setTerg_exrc_code(row[16]);

		return record;
	}

	@GetMapping("/addCanadaCalling")
	public String addCanadaCalling() throws InterruptedException, ExecutionException {
		log.debug("Import Canada Calling Data to Cloud Firestore");

		final Map<String, CanadaCalling> canadaCallingRecords = new HashMap<>();
		// Checked already
		String inputFile = System.getProperty("lca.report.file.canadacalling");

		try (Stream<String> stream = Files.lines(Paths.get(inputFile), StandardCharsets.UTF_8)) {
			stream.skip(1) // first line is the header -- skip
					.map(x -> x.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1)) // https://stackabuse.com/regex-splitting-by-character-unless-in-quotes/
					.forEach(x -> {
						// System.out.println(x.length);
						String npanxx = x[28];
						CanadaCalling record = getCanadaCallingRecord(x);
						if (!canadaCallingRecords.containsKey(npanxx)) {
							canadaCallingRecords.put(npanxx, record);
						} else {
							CanadaCalling currentRecord = canadaCallingRecords.get(npanxx);
							System.out.println("Find duplicate npanxx: " + npanxx);
							System.out.println(
									"Current has effective date: " + currentRecord.getCoc_effective_date_convert());
							System.out.println("New date: " + x[2]);
							if (currentRecord.getCoc_effective_date_convert().compareTo(x[2]) < 0) {
								canadaCallingRecords.put(npanxx, record);
								System.out.println("New date is used.");
							}
						}
					});
			stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("Total records after removing duplicates is: " + canadaCallingRecords.size());

		FirestoreOptions firestoreOptions = FirestoreOptions.getDefaultInstance().toBuilder().build();

		try (Firestore db = firestoreOptions.getService();) {

			// Convert all Map keys to a List
			List<String> keyList = new ArrayList<>(canadaCallingRecords.keySet());

			// Convert all Map values to a List
			List<CanadaCalling> valueList = new ArrayList<>(canadaCallingRecords.values());
			int batchSize = 480;
			int totalRecords = canadaCallingRecords.size();
			int addedRecords = 0;
			while (addedRecords < totalRecords) {
				// Get a new write batch
				WriteBatch batch = db.batch();
				int addedInBatch;
				for (addedInBatch = 0; addedInBatch < batchSize
						&& (addedInBatch + addedRecords) < totalRecords; addedInBatch++) {
					DocumentReference docRef = db.collection("npanxxs")
							.document(keyList.get(addedInBatch + addedRecords));
					batch.set(docRef, valueList.get(addedInBatch + addedRecords));
				}
				// asynchronously commit the batch
				ApiFuture<List<WriteResult>> future = batch.commit();
				// ...
				// future.get() blocks on batch commit operation
				/*
				 * for (WriteResult result :future.get()) { System.out.println("Update time : "
				 * + result.getUpdateTime()); }
				 */
				future.get();
				addedRecords += addedInBatch;
				System.out.println("Added records: " + addedRecords);
			}
			db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			renameFile(inputFile);
		} catch (IOException e) {
			System.out.println("Exception: renaming file " + inputFile);
			e.printStackTrace();
		}

		return "Added!";
	}

	private void renameFile(final String inputFile) throws IOException {
		String processedFile = inputFile.substring(0, inputFile.length() - 4) + "_processed.csv";
		Path processedFilePath = Paths.get(processedFile);
		if (Files.exists(processedFilePath)) {
			String message = "File " + processedFile + " exists already.";
			System.out.println(message);
			String newFileName = processedFile + "." + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
			Files.move(processedFilePath, Paths.get(newFileName));
			String movedMessage = "File " + processedFile + " is renamed as " + newFileName + ".";
			System.out.println(movedMessage);
		}
		Files.move(Paths.get(inputFile), processedFilePath);
		String movedMessage = "File " + inputFile + " is renamed as " + processedFile + ".";
		System.out.println(movedMessage);
	}

	@PostMapping("/user")
	public Object addUser(@RequestBody User user) throws IOException, InterruptedException, ExecutionException {
		log.debug("Adding Data to Cloud Firestore");

		if (user.getId() == null || user.getId().trim().length() == 0) {
			log.error("User ID cannot be blank!");
			return "Invalid Data.";
		}

		user.setTimestamp(Timestamp.now().toString());

		Firestore db = getFirestore();
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
			if ((String.class.isAssignableFrom(field.getType()) && field.get(user) != null
					&& !field.getName().equals("id"))
					|| (int.class.equals(field.getType()) && (Integer) field.get(user) != 0)) {
				System.out.println(field.getName() + " will be updated");
				updates.put(field.getName(), field.get(user));
			}
		}
		updates.put("timestamp", Timestamp.now().toString());

		Firestore db = getFirestore();
		DocumentReference docRef = db.collection("users").document(user.getId());

		// asynchronously write data
		ApiFuture<WriteResult> result = docRef.update(updates);
		// ...
		// result.get() blocks on response
		System.out.println("Update time : " + result.get().getUpdateTime());

		return docRef.get().get().toObject(User.class);
	}

	@GetMapping(path = "/{collection}", produces = "application/json")
	public Object getUser(@PathVariable String collection, @RequestParam Map<String, String> queryParams)
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

		// Reference - https://cloud.google.com/firestore/docs/quickstart-servers
		FirestoreOptions firestoreOptions = FirestoreOptions.getDefaultInstance().toBuilder().build();
		Firestore db = firestoreOptions.getService();

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
			query = db.collection(collection).get();
			break;
		case 1:
			query = db.collection(collection).whereEqualTo(keys.get(0), values.get(0)).get();
			break;
		case 2:
			query = db.collection(collection).whereEqualTo(keys.get(0), values.get(0))
					.whereEqualTo(keys.get(1), values.get(1)).get();
			break;
		case 3:
			query = db.collection(collection).whereEqualTo(keys.get(0), values.get(0))
					.whereEqualTo(keys.get(1), values.get(1)).whereEqualTo(keys.get(2), values.get(2)).get();
			break;
		default:
			return "Keys size is invalid.";
		}

		// ...
		// query.get() blocks on response
		QuerySnapshot querySnapshot = query.get();
		List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();

		List<Object> records = new ArrayList<>();
		int num = 0;
		for (QueryDocumentSnapshot document : documents) {
			if (num < 20) {
				records.add(document.toObject(Object.class));
			}
			num++;
		}

		String jsonPrettyOutput = new GsonBuilder().setPrettyPrinting().create().toJson(records);
		String outputMessage = null;
		if (num == 0) {
			outputMessage = "There is no such collection in Cloud Firestore: " + collection;
		} else if (num == 1) {
			outputMessage = "There is one document from collection of " + collection + System.lineSeparator()
					+ jsonPrettyOutput;
		} else if (num < 20) {
			outputMessage = "There are " + num + " documents from collection of " + collection + System.lineSeparator()
					+ jsonPrettyOutput;
		} else {
			outputMessage = "Number of documents read from Cloud Firestore in Java structure: " + num
					+ System.lineSeparator() + "First 20: " + System.lineSeparator() + jsonPrettyOutput;
		}

		log.debug(outputMessage);

		return outputMessage;
	}

	@GetMapping(path = "/{collection}/{id}", produces = "application/json")
	public Object getDocumentById(@PathVariable String collection, @PathVariable String id)
			throws IOException, InterruptedException, ExecutionException {
		log.debug("Getting Data by ID from Cloud Firestore");

		Firestore db = getFirestore();
		ApiFuture<DocumentSnapshot> getResult = db.collection(collection).document(id).get();
		DocumentSnapshot document = getResult.get();
		if (document.exists()) {
			return document.toObject(Object.class);
		} else {
			return "No such document!";
		}
	}

	@DeleteMapping("/user/{id}")
	public String deleteUser(@PathVariable String id) throws IOException, InterruptedException, ExecutionException {
		log.debug("Deleting Data from Cloud Firestore");

		Firestore db = getFirestore();
		ApiFuture<WriteResult> deleteResult = db.collection("users").document(id).delete();
		return "Document Deleted Successfully at " + deleteResult.get().getUpdateTime() + ".";

	}

	@SuppressWarnings("serial")
	@DeleteMapping("/user/{doc}/{field}")
	public String deleteUserField(@PathVariable String doc, @PathVariable String field)
			throws IOException, InterruptedException, ExecutionException {
		log.debug("Deleting Data Field from Cloud Firestore");

		Firestore db = getFirestore();
		ApiFuture<WriteResult> updateResult = db.collection("users").document(doc)
				.update(new HashMap<String, Object>() {
					{
						put(field, FieldValue.delete());
						put("timestamp", Timestamp.now().toString());
					}
				});
		return "Field Deleted Successfully at " + updateResult.get().getUpdateTime() + ".";

	}

	private CanadaCalling getCanadaCallingRecord(final String[] row) {
		CanadaCalling record = new CanadaCalling();

		record.setTra_date(row[0]);
		record.setCoc_lest_code(row[1]);
		record.setCoc_effective_date_convert(row[2]);
		record.setCoc_npa_code_1(Integer.parseInt(row[3]));
		record.setCoc_nxx_code_1(Integer.parseInt(row[4]));
		record.setCoc_stat_code(row[5]);
		record.setCoc_opco_code(row[6]);
		record.setOpco_name(row[7]);
		record.setOpco_opct_code_1(row[8]);
		record.setCoc_coty_code(row[9]);
		record.setCoc_ss_code(row[10]);
		record.setCoc_portability_flag_1(row[11]);
		record.setCoc_aocn_opco_code(row[12]);
		record.setERC_Full_Name(row[13]);
		record.setERC_Locality(row[14]);
		record.setERC_Major_V(row[15]);
		record.setERC_Major_H(row[16]);
		record.setCoc_lrn_code_a(row[17]);
		record.setCoc_swit_clli_code(row[18]);
		record.setCoc_sha_code(row[19]);
		record.setHost_sw(row[20]);
		record.setActual_sw(row[21]);
		record.setCall_agent_sw(row[22]);
		record.setSwho_intermediate_tandem_clli_code_1(row[23]);
		record.setTextbox85(row[24]);
		record.setTextbox81(row[25]);
		record.setLir(row[26]);
		record.setLir_tandem(row[27]);
		record.setNpanxx(Integer.parseInt(row[28]));
		record.setTpm_billto_rao_code(row[29]);
		record.setTpm_tpnt_code(row[30]);

		return record;
	}

	private Firestore getFirestore() {
		// old way:
		// FirestoreOptions firestoreOptions =
		// FirestoreOptions.getDefaultInstance().toBuilder().build();

		// another way:
		/*
		 * // Use a service account InputStream serviceAccount = new FileInputStream(
		 * "C:\\dev\\projects\\lca\\batch\\\\gcp_service_account\\Users-e0a6db6e4593.json"
		 * ); GoogleCredentials credentials =
		 * GoogleCredentials.fromStream(serviceAccount); FirebaseOptions options = new
		 * FirebaseOptions.Builder().setCredentials(credentials).build(); if
		 * (FirebaseApp.getApps().isEmpty()) { FirebaseApp.initializeApp(options); }
		 * 
		 * Firestore db = FirestoreClient.getFirestore();
		 */
		FirestoreOptions firestoreOptions = FirestoreOptions.getDefaultInstance();
		return firestoreOptions.getService();
	}

}
