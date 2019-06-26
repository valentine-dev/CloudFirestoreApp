echo set required environment variables - please change...

set LCA_GCP_USE_PROXY=true
set LCA_PROXY_HOST=198.161.14.25
set LCA_PROXY_PORT=8080
set LCA_GOOGLE_APPLICATION_CREDENTIALS=C:\dev\projects\lca\batch\gcp_service_account\Users-e0a6db6e4593.json
set LCA_LOCAL_CALLING_LOOKUP_FILE=C:\dev\projects\lca\batch\report\lc_lookup.csv
set LCA_LOCAL_CALLING_REPORT_FILE=C:\dev\projects\lca\batch\report\LocalCalling.csv
set LCA_CANADA_CALLING_REPORT_FILE=C:\dev\projects\lca\batch\report\TelusCanada.csv

pause

echo run web application...
echo command: java -jar CloudFirestoreApp.jar 

pause