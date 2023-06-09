package com.appdynamics.monitors.kubernetes.SnapshotTasks;

import static com.appdynamics.monitors.kubernetes.Constants.CONFIG_RECS_BATCH_SIZE;
import static com.appdynamics.monitors.kubernetes.Constants.CONFIG_SCHEMA_DEF_POD_STATUS_MONITOR;
import static com.appdynamics.monitors.kubernetes.Constants.CONFIG_SCHEMA_NAME_POD_STATUS_MONITOR;
import static com.appdynamics.monitors.kubernetes.Constants.OPENSHIFT_VERSION;
import static com.appdynamics.monitors.kubernetes.Utilities.ALL;
import static com.appdynamics.monitors.kubernetes.Utilities.checkAddInt;
import static com.appdynamics.monitors.kubernetes.Utilities.checkAddObject;
import static com.appdynamics.monitors.kubernetes.Utilities.ensureSchema;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.appdynamics.extensions.TasksExecutionServiceProvider;
import com.appdynamics.extensions.metrics.Metric;
import com.appdynamics.extensions.util.AssertUtils;
import com.appdynamics.monitors.kubernetes.Utilities;
import com.appdynamics.monitors.kubernetes.Metrics.UploadMetricsTask;
import com.appdynamics.monitors.kubernetes.Models.AppDMetricObj;
import com.appdynamics.monitors.kubernetes.Models.SummaryObj;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.AppsV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1NodeList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodCondition;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1ReplicaSet;
import io.kubernetes.client.util.Config;


public class PodStatusMonitorSnapshotRunner extends SnapshotRunnerBase {

	@Override
    protected SummaryObj initDefaultSummaryObject(Map<String, String> config){
        return initPodStatusMonitorSummaryObject(config, ALL,ALL);
    }
	

	public PodStatusMonitorSnapshotRunner(){

    }

    public PodStatusMonitorSnapshotRunner(TasksExecutionServiceProvider serviceProvider, Map<String, String> config, CountDownLatch countDownLatch){
        super(serviceProvider, config, countDownLatch);
    }

    @SuppressWarnings("unchecked")
	@Override
	public void run() {
	    AssertUtils.assertNotNull(getConfiguration(), "The job configuration cannot be empty");
	    generatePodStatusMonitorSnapshot();
	}

    
	private void generatePodStatusMonitorSnapshot() {
	    logger.info("Proceeding to capture POD Status Monitor snapshot...");

	    Map<String, String> config = (Map<String, String>) getConfiguration().getConfigYml();
	    if (config != null) {
	        String apiKey = Utilities.getEventsAPIKey(config);
	        String accountName = Utilities.getGlobalAccountName(config);
	        URL publishUrl = ensureSchema(config, apiKey, accountName,CONFIG_SCHEMA_NAME_POD_STATUS_MONITOR, CONFIG_SCHEMA_DEF_POD_STATUS_MONITOR);

	        try {
	            V1NodeList nodes = getNodesFromKubernetes(config);

	            createPayload(nodes, config, publishUrl, accountName, apiKey);
	            List<Metric> metricList = getMetricsFromSummary(getSummaryMap(), config);

	            logger.info("About to send {} POD Status Monitor metrics", metricList.size());
	            UploadMetricsTask metricsTask = new UploadMetricsTask(getConfiguration(), getServiceProvider().getMetricWriteHelper(), metricList, countDownLatch);
	            getConfiguration().getExecutorService().execute("UploadPodStatusMonitorMetricsTask", metricsTask);
	        } catch (IOException e) {
	            countDownLatch.countDown();
	            logger.error("Failed to push POD Status Monitor data", e);
	        } catch (Exception e) {
	            countDownLatch.countDown();
	            logger.error("Failed to push POD Status Monitor data", e);
	        }
	    }
	}
	
	public int getNotRunningPodCount(String nodeName, Map<String, String> config) throws Exception {
	    int count = 0;
	    try {
	        ApiClient client = Utilities.initClient(config);
	        this.setAPIServerTimeout(client, K8S_API_TIMEOUT);
	        Configuration.setDefaultApiClient(client);
	        CoreV1Api coreV1Api = new CoreV1Api(client);
	       
	        String podPhase = "Running";
	        V1PodList podList = coreV1Api.listPodForAllNamespaces(null, null, null, null, null, null, null, null, null, null);

	        for (V1Pod pod : podList.getItems()) {
	            if (!pod.getStatus().getPhase().equalsIgnoreCase(podPhase)) {
	                count++;
	            }
	        }
	        return count;
	    } catch (Exception ex) {
	        throw new Exception("Unable to connect to Kubernetes API server because it may be unavailable or the cluster credentials are invalid", ex);
	    }
	}


	public ArrayNode createPayload(V1NodeList nodeList, Map<String, String> config, URL publishUrl, String accountName, String apiKey) throws Exception {
	    ObjectMapper mapper = new ObjectMapper();
	    ArrayNode arrayNode = mapper.createArrayNode();
	    long batchSize = Long.parseLong(config.get(CONFIG_RECS_BATCH_SIZE));

	    


	        List<V1Pod> pods = getPodsFromNode(config);
	      
        
	        for (V1Pod pod : pods) {
	        	ObjectNode objectNode = mapper.createObjectNode();
	  	      if(!OPENSHIFT_VERSION.isEmpty()) {
	              	objectNode = checkAddObject(objectNode,OPENSHIFT_VERSION, "openshiftVersion");
	              }
	           
	             
        	   objectNode = checkAddObject(objectNode, Utilities.getHall(pod), "hall");
               objectNode = checkAddObject(objectNode,pod.getSpec().getNodeName(), "nodeName");
               
               objectNode = checkAddObject(objectNode, pod.getMetadata().getNamespace(), "namespace");
	            
	            objectNode = checkAddObject(objectNode, getContainerName(pod), "containerName");
	            objectNode = checkAddObject(objectNode, getDeploymentName(pod), "deploymentName");
	            objectNode = checkAddObject(objectNode, pod.getMetadata().getName(), "podName");
	            objectNode = checkAddObject(objectNode, getErrorReason(pod), "errorReason");
	            objectNode = checkAddObject(objectNode, getContainerPhase(pod), "containerPhase");
	            String clusterName = Utilities.ensureClusterName(config, pod.getMetadata().getClusterName());
		        objectNode = checkAddObject(objectNode, clusterName, "clusterName");
		      
		        SummaryObj summary = getSummaryMap().get(ALL);


	  
	            if (summary == null) {
	                summary = initPodStatusMonitorSummaryObject(config, ALL, ALL);
	                getSummaryMap().put(ALL, summary);
	            }

	           
	            String namespace = pod.getMetadata().getNamespace();
	            String nodeName = pod.getSpec().getNodeName();
	            SummaryObj summaryNamespace = getSummaryMap().get(namespace);
	            if (Utilities.shouldCollectMetricsForNode(getConfiguration(), namespace)) {
	                if (summaryNamespace == null) {
	                    summaryNamespace = initPodStatusMonitorSummaryObject(config, namespace, ALL);
	                    getSummaryMap().put(namespace, summaryNamespace);
	                }
	            }

	            SummaryObj summaryNode = getSummaryMap().get(nodeName);
	            if (Utilities.shouldCollectMetricsForNode(getConfiguration(), nodeName)) {
	                if (summaryNode == null) {
	                    summaryNode = initPodStatusMonitorSummaryObject(config, ALL, nodeName);
	                    getSummaryMap().put(nodeName, summaryNode);
	                }
	            }
	            
	            int notRunningCount=0;
	            if (!"Running".equalsIgnoreCase(pod.getStatus().getPhase())) {
	            	objectNode = checkAddInt(objectNode, notRunningCount, "NotRunningPodCount");
		        	notRunningCount++;
		        	Utilities.incrementField(summary, "NotRunningPodCount");
		            Utilities.incrementField(summaryNamespace, "NotRunningPodCount");
		            Utilities.incrementField(summaryNode, "NotRunningPodCount");
			    }
	           
	            
	            
	            int podRestarts = 0;

	        
	            if (pod.getStatus().getContainerStatuses() != null){
	                for(V1ContainerStatus status : pod.getStatus().getContainerStatuses()){

	                

	                    int restarts = status.getRestartCount();
	                    podRestarts += restarts;
	                    
	                }
	                //container data


	                
	            }
	            
	            objectNode = checkAddInt(objectNode, podRestarts, "RestartCount");
                Utilities.incrementField(summary, "RestartCount", podRestarts);
                Utilities.incrementField(summaryNamespace, "RestartCount", podRestarts);
                Utilities.incrementField(summaryNode, "RestartCount", podRestarts);
	            arrayNode.add(objectNode);

	            if (arrayNode.size() >= batchSize) {
	                logger.info("Sending batch of {} POD Status Monitor records", arrayNode.size());
	                String payload = arrayNode.toString();
	                arrayNode = arrayNode.removeAll();
	                if (!payload.equals("[]")) {
	                    UploadEventsTask uploadEventsTask = new UploadEventsTask(getTaskName(), config, publishUrl, accountName, apiKey, payload);
	                    getConfiguration().getExecutorService().execute("UploadPodStatusMonitorData", uploadEventsTask);
	                }
	            }
	        }
	        
	      
	   

	    if (arrayNode.size() > 0) {
	        logger.info("Sending last batch of {} POD Status Monitor records", arrayNode.size());
	        String payload = arrayNode.toString();
	        arrayNode = arrayNode.removeAll();
	        if (!payload.equals("[]")) {
	            UploadEventsTask uploadEventsTask = new UploadEventsTask(getTaskName(), config, publishUrl, accountName, apiKey, payload);
	            getConfiguration().getExecutorService().execute("UploadPodStatusMonitorData", uploadEventsTask);
	        }
	    }

	    return arrayNode;
	}

	private String getContainerName(V1Pod pod) {
		String containerNames="";
		boolean flag=false;
		for(V1Container container : pod.getSpec().getContainers()){
			containerNames+=container.getName()+", ";
			flag=true;
		}
	    if(flag)
		containerNames.substring(0,containerNames.length()-1);
	    return containerNames;
	}

	private String getDeploymentName(V1Pod pod) throws ApiException {
		 // Get the metadata of the pod
        V1ObjectMeta metadata = pod.getMetadata();

        // Get the owner references from the pod's metadata
        List<V1OwnerReference> ownerReferences = metadata.getOwnerReferences();
        


        AppsV1Api appsApi = new AppsV1Api();
        
        // Find the owner reference with the "ReplicaSet" kind
        V1OwnerReference replicaSetReference = null;
        if (ownerReferences !=null) {
            for (V1OwnerReference ref : ownerReferences) {
                if (ref.getKind().equals("ReplicaSet")) {
                    replicaSetReference = ref;
                    break;
                }
            }

            if (replicaSetReference != null) {
                String replicaSetName = replicaSetReference.getName();

                // Get the ReplicaSet object
                V1ReplicaSet replicaSet = appsApi.readNamespacedReplicaSet(replicaSetName,pod.getMetadata().getNamespace(), null);

                // Get the owner references from the ReplicaSet's metadata
                List<V1OwnerReference> rsOwnerReferences = replicaSet.getMetadata().getOwnerReferences();

                // Find the owner reference with the "Deployment" kind
                V1OwnerReference deploymentReference = null;
                if(rsOwnerReferences!=null) {
	                for (V1OwnerReference ref : rsOwnerReferences) {
	                    if (ref.getKind().equals("Deployment")) {
	                        deploymentReference = ref;
	                        break;
	                    }
	                }
                }
                else {
                	
                	logger.info("rsOwnerReferences is null for {} and {}",replicaSetName , replicaSet.getMetadata().getName() );
                }

                if (deploymentReference != null) {
                    String deploymentName = deploymentReference.getName();
                   return deploymentName;
                }
            } 
        }
		return "";
	}
	


	private String getErrorReason(V1Pod pod) {
		String reasons="";
	    if (pod.getStatus() != null && pod.getStatus().getConditions() != null) {
	        List<V1PodCondition> conditions = pod.getStatus().getConditions();
	        for (V1PodCondition condition : conditions) {
	            if ( condition.getStatus().equalsIgnoreCase("False")) {
	                // Retrieve the error reason from the pod's condition
	            	reasons += String.format("%s;", condition.getReason()+", Message: "+condition.getMessage());
	            }
	        }
	    }
	    return reasons;
	}

	private String getContainerPhase(V1Pod pod) {
	    if (pod.getStatus() != null) {
	        String phase = pod.getStatus().getPhase();
	        // Retrieve the container phase from the pod's status phase
	        return phase;
	    }
	    return "";
	}



	
	private List<V1Pod> getPodsFromNode(Map<String, String> config) throws Exception {
		V1PodList podList;
		ApiClient client = Utilities.initClient(config);
         this.setAPIServerTimeout(client, K8S_API_TIMEOUT);
         Configuration.setDefaultApiClient(client);
         CoreV1Api api = new CoreV1Api();

         this.setCoreAPIServerTimeout(api, K8S_API_TIMEOUT);
         podList = api.listPodForAllNamespaces(null,
                 null,
                 null,
                 null,
                 null,
                 null,
                 null,
                 null,
                 null, null);

	    return podList.getItems();
	}


	


	private V1NodeList getNodesFromKubernetes(Map<String, String> config) throws Exception {
		  V1NodeList nodeList;

		try {
		ApiClient client = Utilities.initClient(config);
        this.setAPIServerTimeout(client, K8S_API_TIMEOUT);
        Configuration.setDefaultApiClient(client);
        CoreV1Api api = new CoreV1Api();
        this.setCoreAPIServerTimeout(api, K8S_API_TIMEOUT);
         nodeList = api.listNode(null,
                 false,
                 null,
                 null,
                 null, 500,
                 null,
                 null,
                 K8S_API_TIMEOUT,
                 false);
    }
    catch (Exception ex){
        throw new Exception("Unable to connect to Kubernetes API server because it may be unavailable or the cluster credentials are invalid", ex);
    }
        return nodeList;
	
	}


	
	public static SummaryObj initPodStatusMonitorSummaryObject(Map<String, String> config, String namespace,String node) {
	    ObjectMapper mapper = new ObjectMapper();
	    ObjectNode summary = mapper.createObjectNode();
        summary.put("namespace", namespace);
        summary.put("nodename", node);
	    summary.put("NotRunningPodCount",0);

	    summary.put("RestartCount", 0);

	    ArrayList<AppDMetricObj> metricsList = initMetrics(config, namespace,node);

	    String path = Utilities.getMetricsPath(config, namespace, node);

	    return new SummaryObj(summary, metricsList, path);
	}
	
	public static SummaryObj updatePodStatusMonitorSummaryObject(Map<String, String> config, String namespace,String node) {
	    ObjectMapper mapper = new ObjectMapper();
	    ObjectNode summary = mapper.createObjectNode();
        summary.put("namespace", namespace);
        summary.put("nodename", node);
	    summary.put("NotRunningPodCount",0);

	    summary.put("RestartCount", 0);

	    ArrayList<AppDMetricObj> metricsList = initMetrics(config, namespace,node);

	    String path = Utilities.getMetricsPath(config, namespace, node);

	    return new SummaryObj(summary, metricsList, path);
	}


	public static ArrayList<AppDMetricObj> initMetrics(Map<String, String> config, String namespace,String node) {
	    if (Utilities.ClusterName == null || Utilities.ClusterName.isEmpty()) {
	        return new ArrayList<AppDMetricObj>();
	    }

	    String clusterName = Utilities.ClusterName;
	    String parentSchema = config.get(CONFIG_SCHEMA_NAME_POD_STATUS_MONITOR);
	    String rootPath = String.format("Application Infrastructure Performance|%s|Custom Metrics|Cluster Stats", Utilities.getClusterTierName(config));
	    ArrayList<AppDMetricObj> metricsList = new ArrayList<>();

	    String namespacesCondition = "";
        String nodeCondition = "";
	    if(namespace != null && !namespace.equals(ALL)){
            namespacesCondition = String.format("and namespace = \"%s\"", namespace);
        }

        if(node != null && !node.equals(ALL)){
            nodeCondition = String.format("and nodeName = \"%s\"", node);
        }

        String filter = namespacesCondition.isEmpty() ? nodeCondition : namespacesCondition;
   	    metricsList.add(new AppDMetricObj("NotRunningPodCount", parentSchema,CONFIG_SCHEMA_DEF_POD_STATUS_MONITOR,
                String.format("select * from %s where clusterName = \"%s\" %s ORDER BY creationTimestamp DESC", parentSchema, clusterName, filter), rootPath, namespace, node,null));

   	    metricsList.add(new AppDMetricObj("RestartCount", parentSchema,CONFIG_SCHEMA_DEF_POD_STATUS_MONITOR,
                String.format("select * from %s where clusterName = \"%s\" %s ORDER BY creationTimestamp DESC", parentSchema, clusterName, filter), rootPath, namespace, node,null));

	    
	    return metricsList;
	}


	

}
