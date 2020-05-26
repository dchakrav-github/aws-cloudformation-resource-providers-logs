package software.amazon.logs.loggroup;

import org.mockito.ArgumentMatcher;
import software.amazon.awssdk.services.cloudwatch.model.ResourceNotFoundException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteResourcePolicyRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteRetentionPolicyRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutRetentionPolicyRequest;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteRetentionPolicyResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutRetentionPolicyResponse;
import software.amazon.cloudformation.test.AbstractMockTestBase;

import java.util.Collections;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

public class UpdateHandlerTest extends AbstractMockTestBase<CloudWatchLogsClient> {

    private final UpdateHandler handler = new UpdateHandler();

    final String kmsKeyId = "arn:aws:kms:us-east-2:0123456789012:key/" + UUID.randomUUID().toString();
    final int retentionInDays = 7;
    final String logGroupName = "LogGroup";

    //
    // CreateLogGroup
    //
    private final CreateLogGroupRequest createLogGroupRequest =
        CreateLogGroupRequest.builder()
            .logGroupName(logGroupName)
            .build();
    private final ArgumentMatcher<CreateLogGroupRequest> createLogGroupRequestArgumentMatcher =
        argCmp(createLogGroupRequest);
    private final CreateLogGroupResponse createLogGroupResponse =
        CreateLogGroupResponse.builder().build();

    //
    // PutRetentionPolicy
    //
    private final PutRetentionPolicyRequest putRetentionPolicyRequest =
        PutRetentionPolicyRequest.builder()
            .logGroupName(logGroupName)
            .retentionInDays(retentionInDays)
            .build();
    private final PutRetentionPolicyResponse putRetentionPolicyResponse =
        PutRetentionPolicyResponse.builder().build();
    private final ArgumentMatcher<PutRetentionPolicyRequest> putRetentionPolicyRequestArgumentMatcher =
        argCmp(putRetentionPolicyRequest);

    //
    // DeleteRetentionPolicy
    //
    private final DeleteRetentionPolicyResponse deleteRetentionPolicyResponse =
        DeleteRetentionPolicyResponse.builder().build();
    private final DeleteRetentionPolicyRequest deleteRetentionPolicyRequest =
        DeleteRetentionPolicyRequest.builder()
            .logGroupName(logGroupName)
            .build();
    private final ArgumentMatcher<DeleteRetentionPolicyRequest> deleteRetentionPolicyRequestArgumentMatcher =
        argCmp(deleteRetentionPolicyRequest);

    //
    // DescribeLogGroupsRequest
    //
    private final DescribeLogGroupsRequest describeLogGroupsRequest =
        DescribeLogGroupsRequest.builder()
            .logGroupNamePrefix(logGroupName)
            .build();
    private final ArgumentMatcher<DescribeLogGroupsRequest> describeLogGroupsRequestArgumentMatcher =
        argCmp(describeLogGroupsRequest);

    //
    // LogGroup return values
    //
    private final LogGroup logGroup = LogGroup.builder()
        .logGroupName(logGroupName)
        .retentionInDays(retentionInDays)
        .build();
    private final DescribeLogGroupsResponse describeResponse = DescribeLogGroupsResponse.builder()
        .logGroups(Collections.singletonList(logGroup))
        .build();

    private final LogGroup logGroupWithKMS = LogGroup.builder()
        .logGroupName("LogGroup")
        .retentionInDays(7)
        .kmsKeyId(kmsKeyId)
        .build();
    private final DescribeLogGroupsResponse describeResponseWithKMS = DescribeLogGroupsResponse.builder()
        .logGroups(Collections.singletonList(logGroupWithKMS))
        .build();

    private final LogGroup logGroupWithoutRetention = LogGroup.builder()
        .logGroupName("LogGroup")
        .build();
    private final DescribeLogGroupsResponse describeResponseWithoutRetention = DescribeLogGroupsResponse.builder()
        .logGroups(Collections.singletonList(logGroupWithoutRetention))
        .build();

    public UpdateHandlerTest() {
        super(CloudWatchLogsClient.class);
    }

    @Test
    public void handleRequest_Success() {

        final CloudWatchLogsClient service = getServiceClient();
        when(service.putRetentionPolicy(argThat(putRetentionPolicyRequestArgumentMatcher)))
            .thenReturn(putRetentionPolicyResponse);
        when(service.describeLogGroups(argThat(describeLogGroupsRequestArgumentMatcher)))
            .thenReturn(describeResponse);

        final ResourceModel current = ResourceModel.builder()
                .logGroupName(logGroupName)
                .build();
        final ResourceModel desired = ResourceModel.builder()
            .logGroupName(logGroupName)
            .retentionInDays(retentionInDays)
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(desired)
            .previousResourceState(current)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, null, getLoggerProxy());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getResourceModel()).isNotNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(service).putRetentionPolicy(argThat(putRetentionPolicyRequestArgumentMatcher));
        verify(service).describeLogGroups(argThat(describeLogGroupsRequestArgumentMatcher));
    }

    @Test
    public void handleRequest_Success_RetentionPolicyDeleted() {
        final CloudWatchLogsClient service = getServiceClient();
        when(service.deleteRetentionPolicy(argThat(deleteRetentionPolicyRequestArgumentMatcher)))
            .thenReturn(deleteRetentionPolicyResponse);
        when(service.describeLogGroups(argThat(describeLogGroupsRequestArgumentMatcher)))
            .thenReturn(describeResponseWithoutRetention);

        final ResourceModel desired = ResourceModel.builder()
            .logGroupName(logGroupName)
            .build();
        final ResourceModel current = ResourceModel.builder()
            .logGroupName(logGroupName)
            .retentionInDays(retentionInDays)
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(desired)
            .previousResourceState(current)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, null, getLoggerProxy());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getResourceModel()).isNotNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getResourceModel().getRetentionInDays()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_SuccessNoChange() {
        final LogGroup initialLogGroup = LogGroup.builder()
            .logGroupName("LogGroup")
            .retentionInDays(1)
            .build();
        final DescribeLogGroupsResponse initialDescribeResponse = DescribeLogGroupsResponse.builder()
            .logGroups(Collections.singletonList(initialLogGroup))
            .build();
        final LogGroup logGroup = LogGroup.builder()
            .logGroupName("LogGroup")
            .retentionInDays(1)
            .build();
        final DescribeLogGroupsResponse describeResponse = DescribeLogGroupsResponse.builder()
            .logGroups(Collections.singletonList(logGroup))
            .build();

        doReturn(initialDescribeResponse, describeResponse)
            .when(proxy)
            .injectCredentialsAndInvokeV2(
                ArgumentMatchers.any(),
                ArgumentMatchers.any()
            );

        final ResourceModel model = ResourceModel.builder()
            .logGroupName("LogGroup")
            .retentionInDays(1)
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, null, getLoggerProxy());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getResourceModel()).isEqualToComparingFieldByField(logGroup);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_FailureNotFound_ServiceException() {
        final ResourceModel model = ResourceModel.builder()
            .logGroupName("LogGroup")
            .retentionInDays(7)
            .build();
        final ResourceModel desiredState = ResourceModel.builder()
            .logGroupName("LogGroup")
            .retentionInDays(14)
            .build();

        final PutRetentionPolicyRequest retentionPolicyRequest =
            PutRetentionPolicyRequest.builder()
                .logGroupName("LogGroup")
                .retentionInDays(14)
                .build();
        final ArgumentMatcher<PutRetentionPolicyRequest> argumentMatcher = argCmp(retentionPolicyRequest);
        when(getServiceClient().putRetentionPolicy(argThat(argumentMatcher)))
            .thenThrow(ResourceNotFoundException.builder().build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(desiredState)
            .previousResourceState(model)
            .build();

        assertThrows(software.amazon.cloudformation.exceptions.ResourceNotFoundException.class,
            () -> handler.handleRequest(proxy, request, null, getLoggerProxy()));
    }
}
