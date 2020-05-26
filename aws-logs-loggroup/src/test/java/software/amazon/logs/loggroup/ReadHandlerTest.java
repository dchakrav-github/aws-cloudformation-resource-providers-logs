package software.amazon.logs.loggroup;

import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.cloudformation.exceptions.ResourceNotFoundException;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import static org.mockito.Mockito.*;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.awssdk.services.cloudwatchlogs.model.LogGroup;
import software.amazon.cloudformation.test.AbstractMockTestBase;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ReadHandlerTest extends AbstractMockTestBase<CloudWatchLogsClient> {
    ReadHandler handler = new ReadHandler();

    public ReadHandlerTest() {
        super(CloudWatchLogsClient.class);
    }

    @Test
    public void handleRequest_Success() {
        final LogGroup logGroup = LogGroup.builder()
                .logGroupName("LogGroup")
                .retentionInDays(1)
                .build();
        final DescribeLogGroupsResponse describeResponse = DescribeLogGroupsResponse.builder()
                .logGroups(Collections.singletonList(logGroup))
                .build();

        when(getServiceClient().describeLogGroups(ArgumentMatchers.any(DescribeLogGroupsRequest.class)))
            //
            // Sdk equality needs to match on the configuration override as well for equality
            // Sdk Auth has incorrect equality clauses for StaticProviders, causing this to fail
            // Hence using the is the same type above
            //
            // DescribeLogGroupsRequest.builder().logGroupNamePrefix("LogGroup").overrideConfiguration(configuration).build()))
            //
            //
            .thenReturn(describeResponse);

        final ResourceModel model = ResourceModel.builder()
                .logGroupName("LogGroup")
                .retentionInDays(1)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler
            .handleRequest(proxy, request, null, getLoggerProxy());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_FailureNotFound_EmptyLogGroupResponse() {
        final DescribeLogGroupsResponse describeResponse = DescribeLogGroupsResponse.builder()
                .logGroups(Collections.emptyList())
                .build();

        when(getServiceClient().describeLogGroups(ArgumentMatchers.any(DescribeLogGroupsRequest.class)))
            //
            // Sdk equality needs to match on the configuration override as well for equality
            // Sdk Auth has incorrect equality clauses for StaticProviders, causing this to fail
            // Hence using the is the same type above
            //
            // DescribeLogGroupsRequest.builder().logGroupNamePrefix("LogGroup").overrideConfiguration(configuration).build()))
            //
            //
            .thenReturn(describeResponse);

        final ResourceModel model = ResourceModel.builder()
                .logGroupName("LogGroup")
                .retentionInDays(1)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();

        assertThrows(ResourceNotFoundException.class,
            () -> handler.handleRequest(proxy, request, null, getLoggerProxy()));
    }

    @Test
    public void handleRequest_FailureNotFound_NullLogGroupInput() {
        final ResourceModel model = ResourceModel.builder()
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .build();

        assertThrows(ResourceNotFoundException.class,
            () -> handler.handleRequest(proxy, request, null, getLoggerProxy()));
    }

    @Test
    public void handleRequest_FailureNotFound_NullModel() {
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .build();

        assertThrows(ResourceNotFoundException.class,
            () -> handler.handleRequest(proxy, request, null, getLoggerProxy()));
    }
}
