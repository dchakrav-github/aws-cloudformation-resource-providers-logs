package software.amazon.logs.loggroup;

import software.amazon.awssdk.core.exception.RetryableException;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.cloudwatchlogs.model.AssociateKmsKeyRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.CloudWatchLogsException;
import software.amazon.awssdk.services.cloudwatchlogs.model.CreateLogGroupRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.DisassociateKmsKeyRequest;
import software.amazon.awssdk.services.cloudwatchlogs.model.InvalidParameterException;
import software.amazon.cloudformation.proxy.CallChain;
import software.amazon.cloudformation.proxy.ProgressEvent;

final class Util {
    private Util() {}

    static ProgressEvent<ResourceModel, CallbackContext> createLogGroup(
        final CallChain.Initiator<CloudWatchLogsClient, ResourceModel, CallbackContext> initiator) {

        return initiator
            .translateToServiceRequest(model ->
                CreateLogGroupRequest.builder()
                    .logGroupName(model.getLogGroupName())
                    .build())

            .makeServiceCall((r, c) -> c.injectCredentialsAndInvokeV2(r, c.client()::createLogGroup))
            //
            // Unfortunately create response does not return the Arn. However the primary identifier
            // is the log group name, so we will delegate to ReadHandler to set it up later
            //
            .progress();
    }

    static ProgressEvent<ResourceModel, CallbackContext> updateRetentionInDays(
        final CallChain.Initiator<CloudWatchLogsClient, ResourceModel, CallbackContext> initiator,
        final ProgressEvent<ResourceModel, CallbackContext> event) {
        return
            // if nothing to set, return previous event
            initiator.getResourceModel().getRetentionInDays() == null ? event :
                //
                // Else make the call to update retention in days
                //
                initiator.initiate("logs:UpdateRetentionInDays")
                    .translateToServiceRequest(Translator::translateToPutRetentionPolicyRequest)
                    .makeServiceCall((r, c) -> c.injectCredentialsAndInvokeV2(r, c.client()::putRetentionPolicy))
                    .progress();
    }

    static ProgressEvent<ResourceModel, CallbackContext> deleteRetentionPolicy(
        final CallChain.Initiator<CloudWatchLogsClient, ResourceModel, CallbackContext> initiator) {
        return initiator.initiate("logs:deleteRetentionPolicyRequest")
            .translateToServiceRequest(Translator::translateToDeleteRetentionPolicyRequest)
            .makeServiceCall((r, c) -> c.injectCredentialsAndInvokeV2(r, c.client()::deleteRetentionPolicy))
            .progress();
    }

    static ProgressEvent<ResourceModel, CallbackContext> associateKMSKey(
        final CallChain.Initiator<CloudWatchLogsClient, ResourceModel, CallbackContext> initiator,
        final ProgressEvent<ResourceModel, CallbackContext> event) {
        return initiator.getResourceModel().getKmsKeyArn() == null ? event :
            initiator
                .translateToServiceRequest(m ->
                    AssociateKmsKeyRequest.builder()
                        .logGroupName(m.getLogGroupName())
                        .kmsKeyId(m.getKmsKeyArn())
                        .build())
                //
                // Sometimes when KMS keys is created along with the Log groups it takes a while to be in region
                // We default delay of 5s and 20 tries is sufficient. The KMS resource already waits to existence
                // in region, but there can still be some minor propagation problems. Default model of 5s/20 retries
                // is sufficient to protect from this error. If CMK is disabled, the error will indicate that
                //
                .makeServiceCall((r, c) -> c.injectCredentialsAndInvokeV2(r, c.client()::associateKmsKey))
                .handleError(
                    //
                    // TODO, need to check the error message about disable vs propagation error
                    //
                    (request, exception, client_, model_, context_) -> {
                        if (exception instanceof CloudWatchLogsException) {
                            CloudWatchLogsException cwe = (CloudWatchLogsException) exception;
                            if (cwe.awsErrorDetails().errorMessage().contains("The specified KMS Key Id could not be found")) {
                                throw exception;
                            }

                            if (cwe.awsErrorDetails().errorCode().equals("AccessDeniedException")) {
                                initiator.getLogger().log("Got AccessDeniedException when associating KMS, retrying "
                                    + context_.attempts("logs:AssociateKmsKey"));
                                throw RetryableException.builder().cause(exception).build();
                            }
                        }
                        throw exception;
                    }
                )
                .progress();
    }

    static ProgressEvent<ResourceModel, CallbackContext> disassociateKMSKey(
        final CallChain.Initiator<CloudWatchLogsClient, ResourceModel, CallbackContext> initiator) {
        return initiator.initiate("logs:disassociateKMSKey")
            .translateToServiceRequest(m ->
                DisassociateKmsKeyRequest.builder()
                    .logGroupName(m.getLogGroupName())
                    .build())
            .makeServiceCall((r, c) -> c.injectCredentialsAndInvokeV2(r, c.client()::disassociateKmsKey))
            .handleError(
                //
                // Handle exceptions from the call. If InvalidParameterException error message happens to be
                // for non existent CMK, then we okay to proceed.
                //
                (request, exception, client_, model_, context) -> {
                    if (exception instanceof InvalidParameterException) {
                        //
                        // TODO: check the error message
                        //
                        ProgressEvent.progress(model_, context);
                    }
                    throw exception;
                }
            )
            .progress();
    }


}
