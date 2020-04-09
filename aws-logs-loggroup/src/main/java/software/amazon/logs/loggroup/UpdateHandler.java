package software.amazon.logs.loggroup;

import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.CallChain;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import static software.amazon.logs.loggroup.Util.*;

public class UpdateHandler extends BaseHandler<CallbackContext> {

    @Override
    public ProgressEvent<ResourceModel, CallbackContext> handleRequest(
        final AmazonWebServicesClientProxy proxy,
        final ResourceHandlerRequest<ResourceModel> request,
        final CallbackContext callbackContext,
        final Logger logger) {

        final ResourceModel current = request.getPreviousResourceState();
        final ResourceModel desired = request.getDesiredResourceState();
        final CallbackContext context = callbackContext == null ? new CallbackContext() : callbackContext;
        final CallChain.Initiator<CloudWatchLogsClient, ResourceModel, CallbackContext>
            initiator = proxy.newInitiator(ClientBuilder::getClient, desired, context);

        return
            handleRetentionChange(desired, current, initiator)
            .then(evt -> handleKMSKeyChange(desired, current, initiator, evt))
            .then(evt -> new ReadHandler().handleRequest(proxy, request, callbackContext, logger));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleRetentionChange(
        final ResourceModel desired,
        final ResourceModel current,
        final CallChain.Initiator<CloudWatchLogsClient, ResourceModel, CallbackContext> initiator) {

        if (desired.getRetentionInDays() == null && current.getRetentionInDays() != null) {
            return deleteRetentionPolicy(initiator);
        }
        return updateRetentionInDays(initiator,
            ProgressEvent.progress(initiator.getResourceModel(), initiator.getCallbackContext()));
    }

    protected ProgressEvent<ResourceModel, CallbackContext> handleKMSKeyChange(
        final ResourceModel desired,
        final ResourceModel current,
        final CallChain.Initiator<CloudWatchLogsClient, ResourceModel, CallbackContext> initiator,
        final ProgressEvent<ResourceModel, CallbackContext> chainedEvent) {

        //
        // Handle KMS key changes
        //
        if (desired.getKMSKey() == null && current.getKMSKey() != null) {
            return disassociateKMSKey(initiator);
        }
        else if (desired.getKMSKey() != null && !desired.getKMSKey().equals(current.getKMSKey())) {
            //
            // Change KMS keys association
            //
            return associateKMSKey(initiator, chainedEvent);
        }
        //
        // No change, return chained Event
        //
        return chainedEvent;
    }


}
