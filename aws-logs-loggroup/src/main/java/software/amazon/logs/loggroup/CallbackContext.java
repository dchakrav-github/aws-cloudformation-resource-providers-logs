package software.amazon.logs.loggroup;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import software.amazon.cloudformation.proxy.StdCallbackContext;

@Data
@NoArgsConstructor
@Builder
@EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext {}
