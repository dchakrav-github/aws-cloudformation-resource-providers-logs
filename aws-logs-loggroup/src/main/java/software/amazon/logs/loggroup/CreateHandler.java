package software.amazon.logs.loggroup;

import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.CallChain;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.resource.IdentifierUtils;
import com.amazonaws.util.StringUtils;

import static software.amazon.logs.loggroup.Util.*;

public class CreateHandler extends BaseHandler<CallbackContext> {
    private static final String DEFAULT_LOG_GROUP_NAME_PREFIX = "LogGroup";
    private static final int MAX_LENGTH_LOG_GROUP_NAME = 512;

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(final AmazonWebServicesClientProxy proxy,
                                                                       final ResourceHandlerRequest<ResourceModel> request,
                                                                       final CallbackContext callbackContext,
                                                                       final Logger logger) {

        final ResourceModel model = resourceModel(request);
        final CallbackContext context = callbackContext == null ? new CallbackContext() : callbackContext;
        final CallChain.Initiator<CloudWatchLogsClient, ResourceModel, CallbackContext>
            initiator = proxy.newInitiator(ClientBuilder::getClient, model, context);

        return
            createLogGroup(initiator)
            .then(event -> updateRetentionInDays(initiator, event))
            .then(event -> associateKMSKey(initiator, event))
            .then(event -> new ReadHandler().handleRequest(proxy, request, event.getCallbackContext(), logger));
    }

    /**
     * Since the resource model itself can be null, as well as any of its attributes,
     * we need to prepare a model to safely operate on. This includes:
     *
     * 1. Setting an empty logical resource ID if it is null. Each real world request should
     *    have a logical ID, but we don't want the log name generation to depend on it.
     * 2. Generating a log name if one is not given. This is a createOnly property,
     *    but we generate one if one is not provided.
     */
    private ResourceModel resourceModel(ResourceHandlerRequest<ResourceModel> request) {
        if (request.getDesiredResourceState() == null) {
            request.setDesiredResourceState(new ResourceModel());
        }

        final ResourceModel model = request.getDesiredResourceState();
        final String identifierPrefix = request.getLogicalResourceIdentifier() == null ?
            DEFAULT_LOG_GROUP_NAME_PREFIX :
            request.getLogicalResourceIdentifier();

        if (StringUtils.isNullOrEmpty(model.getLogGroupName())) {
            model.setLogGroupName(
                IdentifierUtils.generateResourceIdentifier(
                    identifierPrefix,
                    request.getClientRequestToken(),
                    MAX_LENGTH_LOG_GROUP_NAME
                )
            );
        }
        return request.getDesiredResourceState();
    }
}
