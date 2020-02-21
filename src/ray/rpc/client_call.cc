#include "ray/rpc/client_call.h"

namespace ray {
namespace rpc {

void ClientCallManager::PollEventsFromCompletionQueue(int index) {
  void *got_tag;
  bool ok = false;
  // Keep reading events from the `CompletionQueue` until it's shutdown.
  // NOTE(edoakes): we use AsyncNext here because for some unknown reason,
  // synchronous cq_.Next blocks indefinitely in the case that the process
  // received a SIGTERM.
  while (true) {
    auto deadline = gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                                 gpr_time_from_millis(250, GPR_TIMESPAN));
    auto status = cqs_[index].AsyncNext(&got_tag, &ok, deadline);
    if (status == grpc::CompletionQueue::SHUTDOWN) {
      break;
    } else if (status == grpc::CompletionQueue::TIMEOUT && shutdown_) {
      // If we timed out and shutdown, then exit immediately. This should not
      // be needed, but gRPC seems to not return SHUTDOWN correctly in these
      // cases (e.g., test_wait will hang on shutdown without this check).
      break;
    } else if (status != grpc::CompletionQueue::TIMEOUT) {
      auto tag = reinterpret_cast<ClientCallTag *>(got_tag);
      tag->GetCall()->SetReturnStatus();
      if (ok && !main_service_.stopped() && !shutdown_) {
        // Post the callback to the main event loop.
        main_service_.post([tag]() {
          tag->GetCall()->OnReplyReceived();
          // The call is finished, and we can delete this tag now.
          delete tag;
        });
      } else {
        delete tag;
      }
    }
  }
}

}  // namespace rpc
}  // namespace ray
