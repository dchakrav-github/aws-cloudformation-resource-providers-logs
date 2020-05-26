package software.amazon.logs.loggroup;

import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.DescribeLogGroupsRequest;
import software.amazon.cloudformation.exceptions.ResourceNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.CallChain;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ReadHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel model = request.getDesiredResourceState();
        if (model == null || StringUtils.isEmpty(model.getLogGroupName())) {
            throw new ResourceNotFoundException(ResourceModel.TYPE_NAME, "");
        }
        final CallbackContext context = callbackContext == null ? new CallbackContext() : callbackContext;
        final CallChain.Initiator<CloudWatchLogsClient, ResourceModel, CallbackContext>
            initiator = proxy.newInitiator(ClientBuilder::getClient, model, context);
        return initiator.initiate("logs:describeLogsGroup")
            .translateToServiceRequest(m -> DescribeLogGroupsRequest.builder().logGroupNamePrefix(m.getLogGroupName()).build())
            .makeServiceCall((r, c) -> c.injectCredentialsAndInvokeV2(r, c.client()::describeLogGroups))
            .done(describeLogGroupsResponse -> {
                if (describeLogGroupsResponse.logGroups().isEmpty()) {
                    throw new ResourceNotFoundException(ResourceModel.TYPE_NAME, model.getPrimaryIdentifier().toString());
                }
                return ProgressEvent.success(Translator.translateForRead(describeLogGroupsResponse), context);
            });
    }
}
