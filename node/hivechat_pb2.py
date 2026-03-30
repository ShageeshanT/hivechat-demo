# This file intentionally re-exports the canonical proto package.
# The stale auto-generated stub that used to live here caused import
# resolution failures because it contained an empty/truncated serialized
# protobuf descriptor.  All code should import from `proto` directly:
#
#     from proto import hivechat_pb2, hivechat_pb2_grpc
#
# This shim keeps any legacy bare `import hivechat_pb2` working by
# forwarding to the real package.

from proto.hivechat_pb2 import *          # noqa: F401,F403
from proto.hivechat_pb2 import DESCRIPTOR # re-export descriptor too
