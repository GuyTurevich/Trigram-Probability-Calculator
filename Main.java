import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;
import software.amazon.awssdk.services.emr.model.StepConfig;

import java.util.LinkedList;

public class Main {
    private final static software.amazon.awssdk.regions.Region region = Region.US_EAST_1;
    public static final EmrClient emrClient = EmrClient.builder().region(region).build();

    public static void main(String[]args){
        LinkedList<StepConfig> stepsConfigs = new LinkedList<>();
        for(int i = 1; i <= 4; i++){
            stepsConfigs.add(configureStep("s3://bucketurevich2/Step" + i + ".jar", "step" + i));
         }
        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
                .instanceCount(9)
                .masterInstanceType(InstanceType.M4_LARGE.toString())
                .slaveInstanceType(InstanceType.M4_LARGE.toString())
                .hadoopVersion("2.7.3")
                .ec2KeyName("vockey")
                .placement(PlacementType.builder().build())
                .keepJobFlowAliveWhenNoSteps(false)
                .build();

        RunJobFlowRequest request = RunJobFlowRequest.builder()
                .name("3gram")
                .instances(instances)
                .steps(stepsConfigs.get(0),stepsConfigs.get(1),stepsConfigs.get(2),stepsConfigs.get(3))
                .logUri("s3n://bucketurevich2/")
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-5.11.0")
                .build();

        RunJobFlowResponse response = emrClient.runJobFlow(request);
        String id = response.jobFlowId();
        System.out.println("Ran job flow with id: " + id);
    }
    private static StepConfig configureStep(String pathInBucket, String stepName) {

        HadoopJarStepConfig step = HadoopJarStepConfig.builder()
                .jar(pathInBucket)
                .build();

        StepConfig stepConfig = StepConfig.builder()
                .name(stepName)
                .hadoopJarStep(step)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        return stepConfig;
    }
}

