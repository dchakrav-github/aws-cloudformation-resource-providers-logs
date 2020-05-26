package software.amazon.logs.loggroup;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.cloudformation.Action;
import software.amazon.cloudformation.proxy.Delay;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.cloudformation.test.CRUDLifecycleTestBase;
import software.amazon.cloudformation.test.HandlerInvoke;
import software.amazon.cloudformation.test.InjectProfileCredentials;
import software.amazon.cloudformation.test.annotations.InjectSessionCredentials;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@ExtendWith(InjectProfileCredentials.class)
@EnabledIfSystemProperty(named = "desktop", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CRUDLifecycleTest extends CRUDLifecycleTestBase<ResourceModel, CallbackContext> {

    private final ReadHandler readHandler = new ReadHandler();
    private final DeleteHandler deleteHandler = new DeleteHandler();
    private final UpdateHandler updateHandler = new UpdateHandler();
    private final CreateHandler createHandler = new CreateHandler();
    private final Map<Action, HandlerInvoke<ResourceModel, CallbackContext>> handlers =
        ImmutableMap.<Action, HandlerInvoke<ResourceModel, CallbackContext>>builder()
            .put(Action.CREATE, createHandler::handleRequest)
            .put(Action.READ, readHandler::handleRequest)
            .put(Action.UPDATE, updateHandler::handleRequest)
            .put(Action.DELETE, deleteHandler::handleRequest)
        .build();

    private final String logGroupName = "logGroup-TEST-DELETE-" + UUID.randomUUID().toString();

    static final Delay override = Constant.of().delay(Duration.ofSeconds(1))
        .timeout(Duration.ofSeconds(10)).build();
    public CRUDLifecycleTest(@InjectSessionCredentials(profile = "cfn-integ") AwsSessionCredentials sessionCredentials) {
        super(sessionCredentials, ((apiCall, provided) -> override));
    }

    @Override
    protected List<ResourceLifecycleSteps> testSeed() {
        final String kmsKeyId = getKmsKeyId();
        final String kmsKeyArn = getKmsKeyArn();
        List<ResourceLifecycleSteps> testGroups = new ArrayList<>(1);

        testGroups.add(
            newStepBuilder()
                .group("simple_normal")
                .create(ResourceModel.builder().logGroupName(logGroupName).build())
                .update(ResourceModel.builder().logGroupName(logGroupName).retentionInDays(7).build())
                .delete(ResourceModel.builder().logGroupName(logGroupName).build()));

        testGroups.add(
            newStepBuilder()
                .group("complex_with_failed_kms")
                .create(ResourceModel.builder().logGroupName(logGroupName).build())
                .createFail(ResourceModel.builder().logGroupName(logGroupName).build())
                .updateFail(ResourceModel.builder().logGroupName(logGroupName).retentionInDays(10).build())
                .update(ResourceModel.builder().logGroupName(logGroupName).retentionInDays(7).build())
                .updateFail(
                    ResourceModel.builder().logGroupName(logGroupName).retentionInDays(7)
                        .kMSKey("kmsKeyDoesNotExist").build())
                //
                // can not access logs service
                //
                .updateFail(() -> {
                    removeServiceAccess("logs", kmsKeyId, Region.US_EAST_2);
                    return ResourceModel.builder().logGroupName(logGroupName).retentionInDays(7)
                        .kMSKey(kmsKeyArn).build();
                })
                .update(() -> {
                    addServiceAccess("logs", kmsKeyId, Region.US_EAST_2);
                    return ResourceModel.builder().logGroupName(logGroupName).retentionInDays(7)
                        .kMSKey(kmsKeyArn).build();
                })
                .delete(ResourceModel.builder().logGroupName(logGroupName).build()));
        return testGroups;
    }

    @Override
    protected Map<Action, HandlerInvoke<ResourceModel, CallbackContext>> handlers() {
        return handlers;
    }

    @Override
    protected CallbackContext context() {
        return new CallbackContext();
    }
}
