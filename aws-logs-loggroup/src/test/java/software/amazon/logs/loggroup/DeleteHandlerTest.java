package software.amazon.logs.loggroup;

import org.mockito.ArgumentMatcher;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.cloudformation.exceptions.ResourceNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
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
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogGroupResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;
import software.amazon.cloudformation.test.AbstractMockTestBase;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractMockTestBase<CloudWatchLogsClient> {
    DeleteHandler handler = new DeleteHandler();

    public DeleteHandlerTest() {
        super(CloudWatchLogsClient.class);
    }

    @Test
    public void handleRequest_Success() {
        final CloudWatchLogsClient client = getServiceClient();
        final DeleteLogGroupResponse deleteResponse =
            DeleteLogGroupResponse.builder().build();
        final DeleteLogGroupRequest deleteLogGroupRequest =
            DeleteLogGroupRequest.builder()
                .logGroupName("LogGroup")
                .build();
        final ArgumentMatcher<DeleteLogGroupRequest> argCmp = argCmp(deleteLogGroupRequest);
        when(client.deleteLogGroup(argThat(argCmp))).thenReturn(deleteResponse);

        // Expect query
        final DescribeLogGroupsRequest describeLogGroupsRequest =
            DescribeLogGroupsRequest.builder()
                .logGroupNamePrefix("LogGroup")
                .build();
        final ArgumentMatcher<DescribeLogGroupsRequest> describeLogGroupsRequestArgumentMatcher =
            argCmp(describeLogGroupsRequest);
        when(client.describeLogGroups(argThat(describeLogGroupsRequestArgumentMatcher)))
            .thenThrow(software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException
                .builder().build());

        final ResourceModel model = ResourceModel.builder()
                .logGroupName("LogGroup")
                .retentionInDays(7)
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
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(client).deleteLogGroup(argThat(argCmp));
        verify(client).describeLogGroups(argThat(describeLogGroupsRequestArgumentMatcher));
    }

    @Test
    public void handleRequest_NotFound_Failure() {
        final CloudWatchLogsClient client = getServiceClient();
        final DeleteLogGroupResponse deleteResponse =
            DeleteLogGroupResponse.builder().build();
        final DeleteLogGroupRequest deleteLogGroupRequest =
            DeleteLogGroupRequest.builder()
                .logGroupName("LogGroup")
                .build();
        final ArgumentMatcher<DeleteLogGroupRequest> argCmp = argCmp(deleteLogGroupRequest);
        when(client.deleteLogGroup(argThat(argCmp)))
            .thenThrow(make(
                software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException.builder(),
                404,
                "LogGroup Not Found",
                software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException.class));

        final ResourceModel model = ResourceModel.builder()
            .logGroupName("LogGroup")
            .retentionInDays(7)
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        try {
            handler.handleRequest(proxy, request, null, getLoggerProxy());
            fail("Expected to get " + ResourceNotFoundException.class);
        } catch (ResourceNotFoundException e) {
            // expected
        }

        verify(client).deleteLogGroup(argThat(argCmp));
    }
}
