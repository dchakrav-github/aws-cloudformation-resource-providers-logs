package software.amazon.logs.loggroup;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.assertj.core.api.Assertions.assertThat;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.cloudformation.proxy.Delay;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.cloudformation.test.InjectProfileCredentials;
import software.amazon.cloudformation.test.InjectSessionCredentials;
import software.amazon.cloudformation.test.KMSKeyEnabledServiceIntegrationTestBase;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ExtendWith(InjectProfileCredentials.class)
@EnabledIfSystemProperty(named = "desktop", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LifecycleTest extends KMSKeyEnabledServiceIntegrationTestBase {

    private final String logGroupName = "logGroup-TEST-DELETE-" + UUID.randomUUID().toString();
    private ResourceModel model = ResourceModel.builder()
        .logGroupName(logGroupName)
        .build();
    static final Delay override = Constant.of().delay(Duration.ofSeconds(1))
        .timeout(Duration.ofSeconds(3)).build();
    public LifecycleTest(@InjectSessionCredentials(profile = "cfn-integ") AwsSessionCredentials awsCredentials) {
        super(awsCredentials, ((apiCall, provided) -> override));
    }


    @Order(10)
    @Test
    public void cleanUp() {
        final ResourceHandlerRequest<ResourceModel> request = createRequest(model);
        // log group existed, let us delete it
        //
        ProgressEvent<ResourceModel, CallbackContext> event = new DeleteHandler().handleRequest(
            getProxy(), request, null, getLoggerProxy()
        );
        assertThat(event.isSuccess()).isTrue();
    }

    @Order(50)
    @Test
    public void createLogGroup() {
        ProgressEvent<ResourceModel, CallbackContext> event = new CreateHandler()
            .handleRequest(getProxy(), createRequest(model), null, getLoggerProxy());
        assertThat(event.isSuccess()).isTrue();
        model = event.getResourceModel();
        assertThat(model.getArn()).isNotNull();
        assertThat(event.getCallbackContext()).isNotNull();
    }

    @Order(100)
    @Test
    public void failRecreateLogGroup() {
        ProgressEvent<ResourceModel, CallbackContext> event = new CreateHandler()
            .handleRequest(getProxy(), createRequest(model), null, getLoggerProxy());
        assertThat(event.isSuccess()).isFalse();
        assertThat(event.getCallbackContext()).isNotNull();
    }

    @Order(150)
    @Test
    public void addRetention() {
        final ResourceModel current = ResourceModel.builder().arn(model.getArn())
            .logGroupName(model.getLogGroupName()).build();
        model.setRetentionInDays(14);
        ProgressEvent<ResourceModel, CallbackContext> event = new UpdateHandler()
            .handleRequest(getProxy(), createRequest(model, current), null, getLoggerProxy());
        model = event.getResourceModel();
        assertThat(event.isSuccess()).isTrue();
    }

    @Order(200)
    @Test
    public void removeRetention() {
        final ResourceModel current = ResourceModel.builder().arn(model.getArn())
            .logGroupName(model.getLogGroupName()).retentionInDays(model.getRetentionInDays()).build();
        model.setRetentionInDays(null);
        ProgressEvent<ResourceModel, CallbackContext> event = new UpdateHandler()
            .handleRequest(getProxy(), createRequest(model, current), null, getLoggerProxy());
        model = event.getResourceModel();
        assertThat(event.isSuccess()).isTrue();
    }

    @Order(250)
    @Test
    void addRetentionWithFailedKMS() {
        final ResourceModel current = ResourceModel.builder().arn(model.getArn())
            .logGroupName(model.getLogGroupName()).retentionInDays(model.getRetentionInDays()).build();
        model.setRetentionInDays(14);
        model.setKMSKey("Does-not-exist");
        ProgressEvent<ResourceModel, CallbackContext> event = new UpdateHandler()
            .handleRequest(getProxy(), createRequest(model, current), null, getLoggerProxy());
        assertThat(event.isFailed()).isTrue();
        CallbackContext context = event.getCallbackContext();
        assertThat(context).isNotNull();
        Map<String, Object> graphs = context.callGraphs();
        assertThat(graphs.containsKey("logs:AssociateKMSKey.request")).isFalse();
        assertThat(graphs.containsKey("logs:UpdateRetentionInDays.request")).isTrue();
        assertThat(graphs.containsKey("logs:UpdateRetentionInDays.response")).isTrue();
        model.setKMSKey(null);
    }

    // TODO make it parameterized
    @Order(300)
    @Test
    void addValidKMS() {
        final ResourceModel current = ResourceModel.builder().arn(model.getArn())
            .logGroupName(model.getLogGroupName()).retentionInDays(model.getRetentionInDays()).build();
        String kmsKeyId = getKmsKeyId();
        String kmsKeyArn = getKmsKeyArn();
        addServiceAccess("logs", kmsKeyId);
        model.setKMSKey(kmsKeyArn);
        ProgressEvent<ResourceModel, CallbackContext> event = new UpdateHandler()
            .handleRequest(getProxy(), createRequest(model, current), null, getLoggerProxy());
        assertThat(event.isSuccess()).isTrue();
        model = event.getResourceModel();
    }

    @Order(310)
    @Test
    void removeKMS() {
        final ResourceModel current = ResourceModel.builder().arn(model.getArn())
            .logGroupName(model.getLogGroupName()).retentionInDays(model.getRetentionInDays())
            .kMSKey(model.getKMSKey())
            .build();
        model.setKMSKey(null);
        ProgressEvent<ResourceModel, CallbackContext> event = new UpdateHandler()
            .handleRequest(getProxy(), createRequest(model, current), null, getLoggerProxy());
        assertThat(event.isSuccess()).isTrue();
        model = event.getResourceModel();
    }

    @Order(350)
    @Test
    void deleteLogGroup() {
        cleanUp();
    }


}
