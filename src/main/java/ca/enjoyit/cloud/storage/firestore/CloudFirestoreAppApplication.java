package ca.enjoyit.cloud.storage.firestore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CloudFirestoreAppApplication {

	private static final Log log = LogFactory.getLog(CloudFirestoreAppApplication.class);
	private static String[] requiredEnvVariables = { 
			"LCA_PROXY_HOST", 
			"LCA_PROXY_PORT", 
			"LCA_GCP_USE_PROXY",
			"LCA_GOOGLE_APPLICATION_CREDENTIALS",
			"LCA_LOCAL_CALLING_LOOKUP_FILE",
			"LCA_LOCAL_CALLING_REPORT_FILE",
			"LCA_CANADA_CALLING_REPORT_FILE"
			};

	public static void main(String[] args) {
		SpringApplication.run(CloudFirestoreAppApplication.class, args);

		checkRequiredEnvironmentVariablesSetting();
	}

	private static void checkRequiredEnvironmentVariablesSetting() {
		for (String env : requiredEnvVariables) {
			log.debug("Checking whether " + env + " is set...");
			if (System.getenv(env) == null) {
				log.error(env + " is not set.");
				System.exit(-1);
			}
			log.debug(env + " is set!");
		}
		System.setProperty("https.proxyHost", System.getenv("LCA_PROXY_HOST"));
		System.setProperty("https.proxyPort", System.getenv("LCA_PROXY_PORT"));
		System.setProperty("com.google.api.client.should_use_proxy", System.getenv("LCA_GCP_USE_PROXY"));
		System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", System.getenv("LCA_GOOGLE_APPLICATION_CREDENTIALS"));
		System.setProperty("lca.report.file.localcalling", System.getenv("LCA_LOCAL_CALLING_REPORT_FILE"));
		System.setProperty("lca.report.file.canadacalling", System.getenv("LCA_CANADA_CALLING_REPORT_FILE"));
		System.setProperty("lca.lookup.file.localcalling", System.getenv("LCA_LOCAL_CALLING_LOOKUP_FILE"));
	}

}
