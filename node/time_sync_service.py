"""
HiveChat - Time Sync gRPC Service
Member: Shagee (IT24103322)

Implements the gRPC server (TimeSyncServicer) and client logic
for NTP-style clock offset estimation between nodes.
"""

import sys
import os
import time
import grpc
from concurrent import futures

# Add proto directory to path so generated stubs can be imported
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'proto'))

import hivechat_pb2
import hivechat_pb2_grpc


class TimeSyncServicer(hivechat_pb2_grpc.TimeSyncServiceServicer):
    """gRPC server handler for time sync requests.

    When a peer calls GetTime(), this servicer responds with the
    current server time so the caller can compute their clock offset.
    """

    def __init__(self, node_id: int):
        self.node_id = node_id

    def GetTime(self, request, context):
        """Handle a time sync request by echoing the client's send time
        and attaching the server's current time.

        The client uses these three values (send_time, server_time, recv_time)
        to compute RTT and clock offset via Cristian's algorithm.
        """
        server_time = time.time()
        return hivechat_pb2.TimeSyncResponse(
            client_send_time=request.client_send_time,
            server_time=server_time,
            server_node_id=self.node_id,
        )


def start_sync_server(node_id: int, port: int) -> grpc.Server:
    """Start a gRPC server that handles TimeSyncService requests.

    Parameters
    ----------
    node_id : int
        This node's identifier.
    port : int
        Port to listen on.

    Returns
    -------
    grpc.Server
        The running server instance (call server.stop() to shut down).
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    servicer = TimeSyncServicer(node_id)
    hivechat_pb2_grpc.add_TimeSyncServiceServicer_to_server(servicer, server)
    actual_port = server.add_insecure_port(f'127.0.0.1:{port}')
    server.start()
    print(f"[TimeSyncService] Node {node_id}: server listening on port {actual_port}")
    return server, actual_port


def sync_once(target_addr: str, node_id: int) -> dict:
    """Perform a single time sync RPC against the target node.

    Parameters
    ----------
    target_addr : str
        The address of the reference node (e.g. "localhost:50051").
    node_id : int
        This node's identifier.

    Returns
    -------
    dict
        Contains 'offset', 'rtt', 'server_node_id' on success,
        or 'error' on failure.
    """
    t_send = time.time()

    try:
        channel = grpc.insecure_channel(target_addr)
        stub = hivechat_pb2_grpc.TimeSyncServiceStub(channel)

        request = hivechat_pb2.TimeSyncRequest(
            node_id=node_id,
            client_send_time=t_send,
        )

        response = stub.GetTime(request, timeout=2.0)
        t_recv = time.time()

        rtt = t_recv - t_send
        offset = response.server_time - (t_send + rtt / 2)

        channel.close()

        return {
            'offset': offset,
            'rtt': rtt,
            'server_node_id': response.server_node_id,
        }
    except grpc.RpcError as e:
        return {'error': str(e)}
