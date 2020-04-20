package software.amazon.logs.loggroup;

import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DeleteLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.CallChain;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.ResourceNotFoundException;

public class DeleteHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        final CallbackContext context = callbackContext == null ? new CallbackContext() : callbackContext;
        final CallChain.Initiator<CloudWatchLogsClient, ResourceModel, CallbackContext>
            initiator = proxy.newInitiator(ClientBuilder::getClient, model, context);

        return initiator.initiate("logs:deleteLogGroup")
            .translate(m -> DeleteLogGroupRequest.builder().logGroupName(m.getLogGroupName()).build())
            .call((r, c) -> {
                try {
                    return c.injectCredentialsAndInvokeV2(r, c.client()::deleteLogGroup);
                } catch (ResourceNotFoundException e) {
                    throw new software.amazon.cloudformation.exceptions.ResourceNotFoundException(e);
                }
            })
            .stabilize((request_, response, client, model_, context_) -> {
                try {
                    DescribeLogGroupsResponse res = client.injectCredentialsAndInvokeV2(
                        DescribeLogGroupsRequest.builder().logGroupNamePrefix(model_.getLogGroupName()).build(),
                        client.client()::describeLogGroups
                    );
                    return res.logGroups().stream().noneMatch(
                        grp -> grp.logGroupName().equals(model_.getLogGroupName()));
                } catch (ResourceNotFoundException e) {
                    return true;
                }
            })
            .done(ignored -> ProgressEvent.success(null, context));

    }
}
