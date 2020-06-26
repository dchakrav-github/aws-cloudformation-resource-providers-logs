package software.amazon.logs.loggroup;

import org.apache.http.HttpStatus;
import org.mockito.ArgumentMatcher;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.AssociateKmsKeyRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.AssociateKmsKeyResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutResourcePolicyRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutRetentionPolicyRequest;
import software.amazon.cloudformation.exceptions.CfnAlreadyExistsException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;
import software.amazon.awssdk.services.cloudwatchlogs.model.PutRetentionPolicyResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceAlreadyExistsException;
import software.amazon.cloudformation.resource.IdentifierUtils;
import software.amazon.cloudformation.test.AbstractMockTestBase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class CreateHandlerTest extends AbstractMockTestBase<CloudWatchLogsClient> {
    private CreateHandler handler = new CreateHandler();

    public CreateHandlerTest() {
        super(CloudWatchLogsClient.class);
    }

    @Test
    public void handleRequest_Success() {

        final CloudWatchLogsClient service = getServiceClient();
        final String logGroupName = "LogGroup";
        final int retentionInDays = 7;
        //
        // create setup
        //
        final CreateLogGroupRequest createLogGroupRequest = CreateLogGroupRequest.builder()
            .logGroupName(logGroupName)
            .build();
        final CreateLogGroupResponse createLogGroupResponse = CreateLogGroupResponse.builder().build();
        final ArgumentMatcher<CreateLogGroupRequest> cLGR = argCmp(createLogGroupRequest);
        when(service.createLogGroup(argThat(cLGR))).thenReturn(createLogGroupResponse);

        //
        // put policy request
        //
        final PutRetentionPolicyRequest putRetentionPolicyRequest =
            PutRetentionPolicyRequest.builder()
                .logGroupName(logGroupName)
                .retentionInDays(retentionInDays)
                .build();
        final PutRetentionPolicyResponse putRetentionPolicyResponse =
            PutRetentionPolicyResponse.builder().build();
        final ArgumentMatcher<PutRetentionPolicyRequest> argMatch = argCmp(putRetentionPolicyRequest);
        when(service.putRetentionPolicy(argThat(argMatch))).thenReturn(putRetentionPolicyResponse);

        //
        // Setup ReadHandler request
        //
        final DescribeLogGroupsRequest describeLogGroupsRequest =
            DescribeLogGroupsRequest.builder()
                .logGroupNamePrefix(logGroupName)
                .build();
        final LogGroup logGroup =
            LogGroup.builder()
                .logGroupName(logGroupName)
                .arn("aws:arn:logs:us-east-2:0123456789012:logs/" + logGroupName)
                .retentionInDays(retentionInDays)
                .build();
        final DescribeLogGroupsResponse describeLogGroupsResponse =
            DescribeLogGroupsResponse.builder()
                .logGroups(logGroup)
                .build();
        final ArgumentMatcher<DescribeLogGroupsRequest> describeLogGroupsRequestArgumentMatcher =
            argCmp(describeLogGroupsRequest);
        when(service.describeLogGroups(argThat(describeLogGroupsRequestArgumentMatcher)))
            .thenReturn(describeLogGroupsResponse);

        final ResourceModel model = ResourceModel.builder()
                .logGroupName(logGroupName)
                .retentionInDays(retentionInDays)
                .build();

        final ResourceHandlerRequest<ResourceModel> request =  ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, null, getLoggerProxy());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(service).createLogGroup(argThat(cLGR));
        verify(service).putRetentionPolicy(argThat(argMatch));
        verify(service).describeLogGroups(argThat(describeLogGroupsRequestArgumentMatcher));
    }

    @Test
    public void handleRequest_SuccessGeneratedLogGroupName_ModelIsNull() {
        //
        // Get the mock
        //
        final CloudWatchLogsClient service = getServiceClient();

        final String clientToken = UUID.randomUUID().toString();
        final String logGroupName = IdentifierUtils.generateResourceIdentifier(
            "LogGroup", clientToken, 512);
        //
        // create setup
        //
        final CreateLogGroupRequest createLogGroupRequest = CreateLogGroupRequest.builder()
            .logGroupName(logGroupName)
            .build();
        final CreateLogGroupResponse createLogGroupResponse = CreateLogGroupResponse.builder().build();
        final ArgumentMatcher<CreateLogGroupRequest> cLGR = argCmp(createLogGroupRequest);
        when(service.createLogGroup(argThat(cLGR))).thenReturn(createLogGroupResponse);

        //
        // Setup ReadHandler request
        //
        final DescribeLogGroupsRequest describeLogGroupsRequest =
            DescribeLogGroupsRequest.builder()
                .logGroupNamePrefix(logGroupName)
                .build();
        final LogGroup logGroup =
            LogGroup.builder()
                .logGroupName(logGroupName)
                .arn("arn:aws:logs:us-east-2:0123456789012:logs/" + logGroupName)
                .build();
        final DescribeLogGroupsResponse describeLogGroupsResponse =
            DescribeLogGroupsResponse.builder()
                .logGroups(logGroup)
                .build();
        final ArgumentMatcher<DescribeLogGroupsRequest> describeLogGroupsRequestArgumentMatcher =
            argCmp(describeLogGroupsRequest);
        when(service.describeLogGroups(argThat(describeLogGroupsRequestArgumentMatcher)))
            .thenReturn(describeLogGroupsResponse);
        final DescribeLogGroupsResponse describeResponseInitial = DescribeLogGroupsResponse.builder()
            .logGroups(Collections.emptyList())
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .clientRequestToken(clientToken)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, null, getLoggerProxy());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        // There isn't an easy way to check the generated value of the name
        assertThat(response.getResourceModel()).isNotNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(service).createLogGroup(argThat(cLGR));
        verify(service).describeLogGroups(argThat(describeLogGroupsRequestArgumentMatcher));
    }

    @Test
    public void associateKMSKey() {
        final CloudWatchLogsClient service = getServiceClient();
        final String logGroupName = "LogGroup";
        final String kmsKeyArn = "arn:aws:kms:us-east-2:0123456789012:key/" + UUID.randomUUID().toString();

        //
        // create setup
        //
        final CreateLogGroupRequest createLogGroupRequest = CreateLogGroupRequest.builder()
            .logGroupName(logGroupName)
            .build();
        final CreateLogGroupResponse createLogGroupResponse = CreateLogGroupResponse.builder().build();
        final ArgumentMatcher<CreateLogGroupRequest> cLGR = argCmp(createLogGroupRequest);
        when(service.createLogGroup(argThat(cLGR))).thenReturn(createLogGroupResponse);
        final AssociateKmsKeyRequest associateKmsKeyRequest =
            AssociateKmsKeyRequest.builder()
                .logGroupName(logGroupName)
                .kmsKeyId(kmsKeyArn)
                .build();
        //
        // Associate Kms Key
        //
        final AssociateKmsKeyResponse associateKmsKeyResponse =
            AssociateKmsKeyResponse.builder().build();
        final ArgumentMatcher<AssociateKmsKeyRequest> associateKmsKeyRequestArgumentMatcher =
            argCmp(associateKmsKeyRequest);
        when(service.associateKmsKey(argThat(associateKmsKeyRequestArgumentMatcher)))
            .thenReturn(associateKmsKeyResponse);

        //
        // Setup ReadHandler request
        //
        final DescribeLogGroupsRequest describeLogGroupsRequest =
            DescribeLogGroupsRequest.builder()
                .logGroupNamePrefix(logGroupName)
                .build();
        final LogGroup logGroup =
            LogGroup.builder()
                .logGroupName(logGroupName)
                .arn("aws:arn:logs:us-east-2:0123456789012:logs/" + logGroupName)
                .kmsKeyId(kmsKeyArn)
                .build();
        final DescribeLogGroupsResponse describeLogGroupsResponse =
            DescribeLogGroupsResponse.builder()
                .logGroups(logGroup)
                .build();
        final ArgumentMatcher<DescribeLogGroupsRequest> describeLogGroupsRequestArgumentMatcher =
            argCmp(describeLogGroupsRequest);
        when(service.describeLogGroups(argThat(describeLogGroupsRequestArgumentMatcher)))
            .thenReturn(describeLogGroupsResponse);

        final ResourceModel model = ResourceModel.builder()
            .logGroupName(logGroupName)
            .kmsKeyArn(kmsKeyArn)
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();
        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, null, getLoggerProxy());


        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(service).createLogGroup(argThat(cLGR));
        verify(service).associateKmsKey(argThat(associateKmsKeyRequestArgumentMatcher));
        verify(service).describeLogGroups(argThat(describeLogGroupsRequestArgumentMatcher));
    }

    @Test
    public void handleRequest_FailureAlreadyExists() {
        final CloudWatchLogsClient service = getServiceClient();
        final String logGroupName = "LogGroup";

        //
        // create setup
        //
        final CreateLogGroupRequest createLogGroupRequest = CreateLogGroupRequest.builder()
            .logGroupName(logGroupName)
            .build();
        final ArgumentMatcher<CreateLogGroupRequest> cLGR = argCmp(createLogGroupRequest);
        when(service.createLogGroup(argThat(cLGR)))
            .thenThrow(make(
                ResourceAlreadyExistsException.builder(),
                400,
                logGroupName + " already exists",
                ResourceAlreadyExistsException.class));

        final ResourceModel model = ResourceModel.builder()
            .logGroupName(logGroupName)
            .build();
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response =
            handler.handleRequest(proxy, request, null, getLoggerProxy());

        assertThat(response).isNotNull();
        assertThat(response.isFailed()).isTrue();
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getMessage()).isNotNull();
        assertThat(response.getMessage().contains(logGroupName + " already exists"));

        verify(service).createLogGroup(argThat(cLGR));
    }
}
