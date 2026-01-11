package org.paxos.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * ---------- Service ----------
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.66.0)",
    comments = "Source: paxos.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class NodeServiceGrpc {

  private NodeServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "paxos.NodeService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.paxos.proto.PrepareMsg,
      org.paxos.proto.PromiseMsg> getPrepareMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Prepare",
      requestType = org.paxos.proto.PrepareMsg.class,
      responseType = org.paxos.proto.PromiseMsg.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.paxos.proto.PrepareMsg,
      org.paxos.proto.PromiseMsg> getPrepareMethod() {
    io.grpc.MethodDescriptor<org.paxos.proto.PrepareMsg, org.paxos.proto.PromiseMsg> getPrepareMethod;
    if ((getPrepareMethod = NodeServiceGrpc.getPrepareMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getPrepareMethod = NodeServiceGrpc.getPrepareMethod) == null) {
          NodeServiceGrpc.getPrepareMethod = getPrepareMethod =
              io.grpc.MethodDescriptor.<org.paxos.proto.PrepareMsg, org.paxos.proto.PromiseMsg>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Prepare"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.PrepareMsg.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.PromiseMsg.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("Prepare"))
              .build();
        }
      }
    }
    return getPrepareMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.paxos.proto.NewViewMsg,
      org.paxos.proto.NewViewAck> getNewViewMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "NewView",
      requestType = org.paxos.proto.NewViewMsg.class,
      responseType = org.paxos.proto.NewViewAck.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.paxos.proto.NewViewMsg,
      org.paxos.proto.NewViewAck> getNewViewMethod() {
    io.grpc.MethodDescriptor<org.paxos.proto.NewViewMsg, org.paxos.proto.NewViewAck> getNewViewMethod;
    if ((getNewViewMethod = NodeServiceGrpc.getNewViewMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getNewViewMethod = NodeServiceGrpc.getNewViewMethod) == null) {
          NodeServiceGrpc.getNewViewMethod = getNewViewMethod =
              io.grpc.MethodDescriptor.<org.paxos.proto.NewViewMsg, org.paxos.proto.NewViewAck>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "NewView"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.NewViewMsg.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.NewViewAck.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("NewView"))
              .build();
        }
      }
    }
    return getNewViewMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.paxos.proto.AcceptMsg,
      org.paxos.proto.AcceptedMsg> getAcceptMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Accept",
      requestType = org.paxos.proto.AcceptMsg.class,
      responseType = org.paxos.proto.AcceptedMsg.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.paxos.proto.AcceptMsg,
      org.paxos.proto.AcceptedMsg> getAcceptMethod() {
    io.grpc.MethodDescriptor<org.paxos.proto.AcceptMsg, org.paxos.proto.AcceptedMsg> getAcceptMethod;
    if ((getAcceptMethod = NodeServiceGrpc.getAcceptMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getAcceptMethod = NodeServiceGrpc.getAcceptMethod) == null) {
          NodeServiceGrpc.getAcceptMethod = getAcceptMethod =
              io.grpc.MethodDescriptor.<org.paxos.proto.AcceptMsg, org.paxos.proto.AcceptedMsg>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Accept"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.AcceptMsg.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.AcceptedMsg.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("Accept"))
              .build();
        }
      }
    }
    return getAcceptMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.paxos.proto.CommitMsg,
      org.paxos.proto.Ack> getCommitMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Commit",
      requestType = org.paxos.proto.CommitMsg.class,
      responseType = org.paxos.proto.Ack.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.paxos.proto.CommitMsg,
      org.paxos.proto.Ack> getCommitMethod() {
    io.grpc.MethodDescriptor<org.paxos.proto.CommitMsg, org.paxos.proto.Ack> getCommitMethod;
    if ((getCommitMethod = NodeServiceGrpc.getCommitMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getCommitMethod = NodeServiceGrpc.getCommitMethod) == null) {
          NodeServiceGrpc.getCommitMethod = getCommitMethod =
              io.grpc.MethodDescriptor.<org.paxos.proto.CommitMsg, org.paxos.proto.Ack>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Commit"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.CommitMsg.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.Ack.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("Commit"))
              .build();
        }
      }
    }
    return getCommitMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.paxos.proto.CatchupRequest,
      org.paxos.proto.CatchupReply> getCatchupMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Catchup",
      requestType = org.paxos.proto.CatchupRequest.class,
      responseType = org.paxos.proto.CatchupReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.paxos.proto.CatchupRequest,
      org.paxos.proto.CatchupReply> getCatchupMethod() {
    io.grpc.MethodDescriptor<org.paxos.proto.CatchupRequest, org.paxos.proto.CatchupReply> getCatchupMethod;
    if ((getCatchupMethod = NodeServiceGrpc.getCatchupMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getCatchupMethod = NodeServiceGrpc.getCatchupMethod) == null) {
          NodeServiceGrpc.getCatchupMethod = getCatchupMethod =
              io.grpc.MethodDescriptor.<org.paxos.proto.CatchupRequest, org.paxos.proto.CatchupReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Catchup"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.CatchupRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.CatchupReply.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("Catchup"))
              .build();
        }
      }
    }
    return getCatchupMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.paxos.proto.HeartbeatMsg,
      org.paxos.proto.Ack> getHeartbeatMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Heartbeat",
      requestType = org.paxos.proto.HeartbeatMsg.class,
      responseType = org.paxos.proto.Ack.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.paxos.proto.HeartbeatMsg,
      org.paxos.proto.Ack> getHeartbeatMethod() {
    io.grpc.MethodDescriptor<org.paxos.proto.HeartbeatMsg, org.paxos.proto.Ack> getHeartbeatMethod;
    if ((getHeartbeatMethod = NodeServiceGrpc.getHeartbeatMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getHeartbeatMethod = NodeServiceGrpc.getHeartbeatMethod) == null) {
          NodeServiceGrpc.getHeartbeatMethod = getHeartbeatMethod =
              io.grpc.MethodDescriptor.<org.paxos.proto.HeartbeatMsg, org.paxos.proto.Ack>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Heartbeat"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.HeartbeatMsg.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.Ack.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("Heartbeat"))
              .build();
        }
      }
    }
    return getHeartbeatMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.paxos.proto.NodeControl,
      org.paxos.proto.Ack> getControlMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Control",
      requestType = org.paxos.proto.NodeControl.class,
      responseType = org.paxos.proto.Ack.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.paxos.proto.NodeControl,
      org.paxos.proto.Ack> getControlMethod() {
    io.grpc.MethodDescriptor<org.paxos.proto.NodeControl, org.paxos.proto.Ack> getControlMethod;
    if ((getControlMethod = NodeServiceGrpc.getControlMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getControlMethod = NodeServiceGrpc.getControlMethod) == null) {
          NodeServiceGrpc.getControlMethod = getControlMethod =
              io.grpc.MethodDescriptor.<org.paxos.proto.NodeControl, org.paxos.proto.Ack>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Control"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.NodeControl.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.Ack.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("Control"))
              .build();
        }
      }
    }
    return getControlMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.paxos.proto.ClientRequest,
      org.paxos.proto.ClientReply> getSubmitMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Submit",
      requestType = org.paxos.proto.ClientRequest.class,
      responseType = org.paxos.proto.ClientReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.paxos.proto.ClientRequest,
      org.paxos.proto.ClientReply> getSubmitMethod() {
    io.grpc.MethodDescriptor<org.paxos.proto.ClientRequest, org.paxos.proto.ClientReply> getSubmitMethod;
    if ((getSubmitMethod = NodeServiceGrpc.getSubmitMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getSubmitMethod = NodeServiceGrpc.getSubmitMethod) == null) {
          NodeServiceGrpc.getSubmitMethod = getSubmitMethod =
              io.grpc.MethodDescriptor.<org.paxos.proto.ClientRequest, org.paxos.proto.ClientReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Submit"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.ClientRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.ClientReply.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("Submit"))
              .build();
        }
      }
    }
    return getSubmitMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.paxos.proto.BalanceRequest,
      org.paxos.proto.BalanceReply> getGetBalanceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetBalance",
      requestType = org.paxos.proto.BalanceRequest.class,
      responseType = org.paxos.proto.BalanceReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.paxos.proto.BalanceRequest,
      org.paxos.proto.BalanceReply> getGetBalanceMethod() {
    io.grpc.MethodDescriptor<org.paxos.proto.BalanceRequest, org.paxos.proto.BalanceReply> getGetBalanceMethod;
    if ((getGetBalanceMethod = NodeServiceGrpc.getGetBalanceMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getGetBalanceMethod = NodeServiceGrpc.getGetBalanceMethod) == null) {
          NodeServiceGrpc.getGetBalanceMethod = getGetBalanceMethod =
              io.grpc.MethodDescriptor.<org.paxos.proto.BalanceRequest, org.paxos.proto.BalanceReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetBalance"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.BalanceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.BalanceReply.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("GetBalance"))
              .build();
        }
      }
    }
    return getGetBalanceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.paxos.proto.TwoPcPrepareMsg,
      org.paxos.proto.TwoPcPreparedReply> getTwoPcPrepareMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TwoPcPrepare",
      requestType = org.paxos.proto.TwoPcPrepareMsg.class,
      responseType = org.paxos.proto.TwoPcPreparedReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.paxos.proto.TwoPcPrepareMsg,
      org.paxos.proto.TwoPcPreparedReply> getTwoPcPrepareMethod() {
    io.grpc.MethodDescriptor<org.paxos.proto.TwoPcPrepareMsg, org.paxos.proto.TwoPcPreparedReply> getTwoPcPrepareMethod;
    if ((getTwoPcPrepareMethod = NodeServiceGrpc.getTwoPcPrepareMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getTwoPcPrepareMethod = NodeServiceGrpc.getTwoPcPrepareMethod) == null) {
          NodeServiceGrpc.getTwoPcPrepareMethod = getTwoPcPrepareMethod =
              io.grpc.MethodDescriptor.<org.paxos.proto.TwoPcPrepareMsg, org.paxos.proto.TwoPcPreparedReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TwoPcPrepare"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.TwoPcPrepareMsg.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.TwoPcPreparedReply.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("TwoPcPrepare"))
              .build();
        }
      }
    }
    return getTwoPcPrepareMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.paxos.proto.TwoPcCommitMsg,
      org.paxos.proto.Ack> getTwoPcCommitMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TwoPcCommit",
      requestType = org.paxos.proto.TwoPcCommitMsg.class,
      responseType = org.paxos.proto.Ack.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.paxos.proto.TwoPcCommitMsg,
      org.paxos.proto.Ack> getTwoPcCommitMethod() {
    io.grpc.MethodDescriptor<org.paxos.proto.TwoPcCommitMsg, org.paxos.proto.Ack> getTwoPcCommitMethod;
    if ((getTwoPcCommitMethod = NodeServiceGrpc.getTwoPcCommitMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getTwoPcCommitMethod = NodeServiceGrpc.getTwoPcCommitMethod) == null) {
          NodeServiceGrpc.getTwoPcCommitMethod = getTwoPcCommitMethod =
              io.grpc.MethodDescriptor.<org.paxos.proto.TwoPcCommitMsg, org.paxos.proto.Ack>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TwoPcCommit"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.TwoPcCommitMsg.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.Ack.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("TwoPcCommit"))
              .build();
        }
      }
    }
    return getTwoPcCommitMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.paxos.proto.TwoPcAbortMsg,
      org.paxos.proto.Ack> getTwoPcAbortMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "TwoPcAbort",
      requestType = org.paxos.proto.TwoPcAbortMsg.class,
      responseType = org.paxos.proto.Ack.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.paxos.proto.TwoPcAbortMsg,
      org.paxos.proto.Ack> getTwoPcAbortMethod() {
    io.grpc.MethodDescriptor<org.paxos.proto.TwoPcAbortMsg, org.paxos.proto.Ack> getTwoPcAbortMethod;
    if ((getTwoPcAbortMethod = NodeServiceGrpc.getTwoPcAbortMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getTwoPcAbortMethod = NodeServiceGrpc.getTwoPcAbortMethod) == null) {
          NodeServiceGrpc.getTwoPcAbortMethod = getTwoPcAbortMethod =
              io.grpc.MethodDescriptor.<org.paxos.proto.TwoPcAbortMsg, org.paxos.proto.Ack>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "TwoPcAbort"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.TwoPcAbortMsg.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.Ack.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("TwoPcAbort"))
              .build();
        }
      }
    }
    return getTwoPcAbortMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      org.paxos.proto.Ack> getResetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Reset",
      requestType = com.google.protobuf.Empty.class,
      responseType = org.paxos.proto.Ack.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      org.paxos.proto.Ack> getResetMethod() {
    io.grpc.MethodDescriptor<com.google.protobuf.Empty, org.paxos.proto.Ack> getResetMethod;
    if ((getResetMethod = NodeServiceGrpc.getResetMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getResetMethod = NodeServiceGrpc.getResetMethod) == null) {
          NodeServiceGrpc.getResetMethod = getResetMethod =
              io.grpc.MethodDescriptor.<com.google.protobuf.Empty, org.paxos.proto.Ack>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Reset"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.Ack.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("Reset"))
              .build();
        }
      }
    }
    return getResetMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      org.paxos.proto.LogDump> getPrintLogMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "printLog",
      requestType = com.google.protobuf.Empty.class,
      responseType = org.paxos.proto.LogDump.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      org.paxos.proto.LogDump> getPrintLogMethod() {
    io.grpc.MethodDescriptor<com.google.protobuf.Empty, org.paxos.proto.LogDump> getPrintLogMethod;
    if ((getPrintLogMethod = NodeServiceGrpc.getPrintLogMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getPrintLogMethod = NodeServiceGrpc.getPrintLogMethod) == null) {
          NodeServiceGrpc.getPrintLogMethod = getPrintLogMethod =
              io.grpc.MethodDescriptor.<com.google.protobuf.Empty, org.paxos.proto.LogDump>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "printLog"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.LogDump.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("printLog"))
              .build();
        }
      }
    }
    return getPrintLogMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      org.paxos.proto.DBDump> getPrintDBMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "printDB",
      requestType = com.google.protobuf.Empty.class,
      responseType = org.paxos.proto.DBDump.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      org.paxos.proto.DBDump> getPrintDBMethod() {
    io.grpc.MethodDescriptor<com.google.protobuf.Empty, org.paxos.proto.DBDump> getPrintDBMethod;
    if ((getPrintDBMethod = NodeServiceGrpc.getPrintDBMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getPrintDBMethod = NodeServiceGrpc.getPrintDBMethod) == null) {
          NodeServiceGrpc.getPrintDBMethod = getPrintDBMethod =
              io.grpc.MethodDescriptor.<com.google.protobuf.Empty, org.paxos.proto.DBDump>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "printDB"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.DBDump.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("printDB"))
              .build();
        }
      }
    }
    return getPrintDBMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      org.paxos.proto.PrintViewDump> getPrintViewMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PrintView",
      requestType = com.google.protobuf.Empty.class,
      responseType = org.paxos.proto.PrintViewDump.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      org.paxos.proto.PrintViewDump> getPrintViewMethod() {
    io.grpc.MethodDescriptor<com.google.protobuf.Empty, org.paxos.proto.PrintViewDump> getPrintViewMethod;
    if ((getPrintViewMethod = NodeServiceGrpc.getPrintViewMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getPrintViewMethod = NodeServiceGrpc.getPrintViewMethod) == null) {
          NodeServiceGrpc.getPrintViewMethod = getPrintViewMethod =
              io.grpc.MethodDescriptor.<com.google.protobuf.Empty, org.paxos.proto.PrintViewDump>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PrintView"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.PrintViewDump.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("PrintView"))
              .build();
        }
      }
    }
    return getPrintViewMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.paxos.proto.StatusRequest,
      org.paxos.proto.StatusDump> getPrintStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PrintStatus",
      requestType = org.paxos.proto.StatusRequest.class,
      responseType = org.paxos.proto.StatusDump.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.paxos.proto.StatusRequest,
      org.paxos.proto.StatusDump> getPrintStatusMethod() {
    io.grpc.MethodDescriptor<org.paxos.proto.StatusRequest, org.paxos.proto.StatusDump> getPrintStatusMethod;
    if ((getPrintStatusMethod = NodeServiceGrpc.getPrintStatusMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getPrintStatusMethod = NodeServiceGrpc.getPrintStatusMethod) == null) {
          NodeServiceGrpc.getPrintStatusMethod = getPrintStatusMethod =
              io.grpc.MethodDescriptor.<org.paxos.proto.StatusRequest, org.paxos.proto.StatusDump>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PrintStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.StatusRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.StatusDump.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("PrintStatus"))
              .build();
        }
      }
    }
    return getPrintStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      org.paxos.proto.SnapshotOffer> getGetLocalSnapshotMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetLocalSnapshot",
      requestType = com.google.protobuf.Empty.class,
      responseType = org.paxos.proto.SnapshotOffer.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.google.protobuf.Empty,
      org.paxos.proto.SnapshotOffer> getGetLocalSnapshotMethod() {
    io.grpc.MethodDescriptor<com.google.protobuf.Empty, org.paxos.proto.SnapshotOffer> getGetLocalSnapshotMethod;
    if ((getGetLocalSnapshotMethod = NodeServiceGrpc.getGetLocalSnapshotMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getGetLocalSnapshotMethod = NodeServiceGrpc.getGetLocalSnapshotMethod) == null) {
          NodeServiceGrpc.getGetLocalSnapshotMethod = getGetLocalSnapshotMethod =
              io.grpc.MethodDescriptor.<com.google.protobuf.Empty, org.paxos.proto.SnapshotOffer>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetLocalSnapshot"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.SnapshotOffer.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("GetLocalSnapshot"))
              .build();
        }
      }
    }
    return getGetLocalSnapshotMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.paxos.proto.SnapshotOffer,
      org.paxos.proto.Ack> getInstallLocalSnapshotMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "InstallLocalSnapshot",
      requestType = org.paxos.proto.SnapshotOffer.class,
      responseType = org.paxos.proto.Ack.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.paxos.proto.SnapshotOffer,
      org.paxos.proto.Ack> getInstallLocalSnapshotMethod() {
    io.grpc.MethodDescriptor<org.paxos.proto.SnapshotOffer, org.paxos.proto.Ack> getInstallLocalSnapshotMethod;
    if ((getInstallLocalSnapshotMethod = NodeServiceGrpc.getInstallLocalSnapshotMethod) == null) {
      synchronized (NodeServiceGrpc.class) {
        if ((getInstallLocalSnapshotMethod = NodeServiceGrpc.getInstallLocalSnapshotMethod) == null) {
          NodeServiceGrpc.getInstallLocalSnapshotMethod = getInstallLocalSnapshotMethod =
              io.grpc.MethodDescriptor.<org.paxos.proto.SnapshotOffer, org.paxos.proto.Ack>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "InstallLocalSnapshot"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.SnapshotOffer.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.paxos.proto.Ack.getDefaultInstance()))
              .setSchemaDescriptor(new NodeServiceMethodDescriptorSupplier("InstallLocalSnapshot"))
              .build();
        }
      }
    }
    return getInstallLocalSnapshotMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static NodeServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<NodeServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<NodeServiceStub>() {
        @java.lang.Override
        public NodeServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new NodeServiceStub(channel, callOptions);
        }
      };
    return NodeServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static NodeServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<NodeServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<NodeServiceBlockingStub>() {
        @java.lang.Override
        public NodeServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new NodeServiceBlockingStub(channel, callOptions);
        }
      };
    return NodeServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static NodeServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<NodeServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<NodeServiceFutureStub>() {
        @java.lang.Override
        public NodeServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new NodeServiceFutureStub(channel, callOptions);
        }
      };
    return NodeServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * ---------- Service ----------
   * </pre>
   */
  public interface AsyncService {

    /**
     */
    default void prepare(org.paxos.proto.PrepareMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.PromiseMsg> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPrepareMethod(), responseObserver);
    }

    /**
     */
    default void newView(org.paxos.proto.NewViewMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.NewViewAck> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getNewViewMethod(), responseObserver);
    }

    /**
     */
    default void accept(org.paxos.proto.AcceptMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.AcceptedMsg> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAcceptMethod(), responseObserver);
    }

    /**
     */
    default void commit(org.paxos.proto.CommitMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.Ack> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCommitMethod(), responseObserver);
    }

    /**
     */
    default void catchup(org.paxos.proto.CatchupRequest request,
        io.grpc.stub.StreamObserver<org.paxos.proto.CatchupReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCatchupMethod(), responseObserver);
    }

    /**
     */
    default void heartbeat(org.paxos.proto.HeartbeatMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.Ack> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getHeartbeatMethod(), responseObserver);
    }

    /**
     */
    default void control(org.paxos.proto.NodeControl request,
        io.grpc.stub.StreamObserver<org.paxos.proto.Ack> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getControlMethod(), responseObserver);
    }

    /**
     */
    default void submit(org.paxos.proto.ClientRequest request,
        io.grpc.stub.StreamObserver<org.paxos.proto.ClientReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSubmitMethod(), responseObserver);
    }

    /**
     */
    default void getBalance(org.paxos.proto.BalanceRequest request,
        io.grpc.stub.StreamObserver<org.paxos.proto.BalanceReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetBalanceMethod(), responseObserver);
    }

    /**
     */
    default void twoPcPrepare(org.paxos.proto.TwoPcPrepareMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.TwoPcPreparedReply> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTwoPcPrepareMethod(), responseObserver);
    }

    /**
     */
    default void twoPcCommit(org.paxos.proto.TwoPcCommitMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.Ack> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTwoPcCommitMethod(), responseObserver);
    }

    /**
     */
    default void twoPcAbort(org.paxos.proto.TwoPcAbortMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.Ack> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getTwoPcAbortMethod(), responseObserver);
    }

    /**
     */
    default void reset(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<org.paxos.proto.Ack> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getResetMethod(), responseObserver);
    }

    /**
     */
    default void printLog(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<org.paxos.proto.LogDump> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPrintLogMethod(), responseObserver);
    }

    /**
     */
    default void printDB(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<org.paxos.proto.DBDump> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPrintDBMethod(), responseObserver);
    }

    /**
     */
    default void printView(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<org.paxos.proto.PrintViewDump> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPrintViewMethod(), responseObserver);
    }

    /**
     */
    default void printStatus(org.paxos.proto.StatusRequest request,
        io.grpc.stub.StreamObserver<org.paxos.proto.StatusDump> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPrintStatusMethod(), responseObserver);
    }

    /**
     */
    default void getLocalSnapshot(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<org.paxos.proto.SnapshotOffer> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetLocalSnapshotMethod(), responseObserver);
    }

    /**
     */
    default void installLocalSnapshot(org.paxos.proto.SnapshotOffer request,
        io.grpc.stub.StreamObserver<org.paxos.proto.Ack> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getInstallLocalSnapshotMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service NodeService.
   * <pre>
   * ---------- Service ----------
   * </pre>
   */
  public static abstract class NodeServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return NodeServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service NodeService.
   * <pre>
   * ---------- Service ----------
   * </pre>
   */
  public static final class NodeServiceStub
      extends io.grpc.stub.AbstractAsyncStub<NodeServiceStub> {
    private NodeServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NodeServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new NodeServiceStub(channel, callOptions);
    }

    /**
     */
    public void prepare(org.paxos.proto.PrepareMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.PromiseMsg> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPrepareMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void newView(org.paxos.proto.NewViewMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.NewViewAck> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getNewViewMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void accept(org.paxos.proto.AcceptMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.AcceptedMsg> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAcceptMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void commit(org.paxos.proto.CommitMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.Ack> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCommitMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void catchup(org.paxos.proto.CatchupRequest request,
        io.grpc.stub.StreamObserver<org.paxos.proto.CatchupReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCatchupMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void heartbeat(org.paxos.proto.HeartbeatMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.Ack> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void control(org.paxos.proto.NodeControl request,
        io.grpc.stub.StreamObserver<org.paxos.proto.Ack> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getControlMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void submit(org.paxos.proto.ClientRequest request,
        io.grpc.stub.StreamObserver<org.paxos.proto.ClientReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSubmitMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getBalance(org.paxos.proto.BalanceRequest request,
        io.grpc.stub.StreamObserver<org.paxos.proto.BalanceReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetBalanceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void twoPcPrepare(org.paxos.proto.TwoPcPrepareMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.TwoPcPreparedReply> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTwoPcPrepareMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void twoPcCommit(org.paxos.proto.TwoPcCommitMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.Ack> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTwoPcCommitMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void twoPcAbort(org.paxos.proto.TwoPcAbortMsg request,
        io.grpc.stub.StreamObserver<org.paxos.proto.Ack> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getTwoPcAbortMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void reset(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<org.paxos.proto.Ack> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getResetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void printLog(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<org.paxos.proto.LogDump> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPrintLogMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void printDB(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<org.paxos.proto.DBDump> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPrintDBMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void printView(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<org.paxos.proto.PrintViewDump> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPrintViewMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void printStatus(org.paxos.proto.StatusRequest request,
        io.grpc.stub.StreamObserver<org.paxos.proto.StatusDump> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPrintStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getLocalSnapshot(com.google.protobuf.Empty request,
        io.grpc.stub.StreamObserver<org.paxos.proto.SnapshotOffer> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetLocalSnapshotMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void installLocalSnapshot(org.paxos.proto.SnapshotOffer request,
        io.grpc.stub.StreamObserver<org.paxos.proto.Ack> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getInstallLocalSnapshotMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service NodeService.
   * <pre>
   * ---------- Service ----------
   * </pre>
   */
  public static final class NodeServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<NodeServiceBlockingStub> {
    private NodeServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NodeServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new NodeServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.paxos.proto.PromiseMsg prepare(org.paxos.proto.PrepareMsg request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPrepareMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.NewViewAck newView(org.paxos.proto.NewViewMsg request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getNewViewMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.AcceptedMsg accept(org.paxos.proto.AcceptMsg request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAcceptMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.Ack commit(org.paxos.proto.CommitMsg request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCommitMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.CatchupReply catchup(org.paxos.proto.CatchupRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCatchupMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.Ack heartbeat(org.paxos.proto.HeartbeatMsg request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getHeartbeatMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.Ack control(org.paxos.proto.NodeControl request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getControlMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.ClientReply submit(org.paxos.proto.ClientRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSubmitMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.BalanceReply getBalance(org.paxos.proto.BalanceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetBalanceMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.TwoPcPreparedReply twoPcPrepare(org.paxos.proto.TwoPcPrepareMsg request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTwoPcPrepareMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.Ack twoPcCommit(org.paxos.proto.TwoPcCommitMsg request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTwoPcCommitMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.Ack twoPcAbort(org.paxos.proto.TwoPcAbortMsg request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getTwoPcAbortMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.Ack reset(com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getResetMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.LogDump printLog(com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPrintLogMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.DBDump printDB(com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPrintDBMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.PrintViewDump printView(com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPrintViewMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.StatusDump printStatus(org.paxos.proto.StatusRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPrintStatusMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.SnapshotOffer getLocalSnapshot(com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetLocalSnapshotMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.paxos.proto.Ack installLocalSnapshot(org.paxos.proto.SnapshotOffer request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getInstallLocalSnapshotMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service NodeService.
   * <pre>
   * ---------- Service ----------
   * </pre>
   */
  public static final class NodeServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<NodeServiceFutureStub> {
    private NodeServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected NodeServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new NodeServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.PromiseMsg> prepare(
        org.paxos.proto.PrepareMsg request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPrepareMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.NewViewAck> newView(
        org.paxos.proto.NewViewMsg request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getNewViewMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.AcceptedMsg> accept(
        org.paxos.proto.AcceptMsg request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAcceptMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.Ack> commit(
        org.paxos.proto.CommitMsg request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCommitMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.CatchupReply> catchup(
        org.paxos.proto.CatchupRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCatchupMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.Ack> heartbeat(
        org.paxos.proto.HeartbeatMsg request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getHeartbeatMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.Ack> control(
        org.paxos.proto.NodeControl request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getControlMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.ClientReply> submit(
        org.paxos.proto.ClientRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSubmitMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.BalanceReply> getBalance(
        org.paxos.proto.BalanceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetBalanceMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.TwoPcPreparedReply> twoPcPrepare(
        org.paxos.proto.TwoPcPrepareMsg request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTwoPcPrepareMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.Ack> twoPcCommit(
        org.paxos.proto.TwoPcCommitMsg request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTwoPcCommitMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.Ack> twoPcAbort(
        org.paxos.proto.TwoPcAbortMsg request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getTwoPcAbortMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.Ack> reset(
        com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getResetMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.LogDump> printLog(
        com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPrintLogMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.DBDump> printDB(
        com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPrintDBMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.PrintViewDump> printView(
        com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPrintViewMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.StatusDump> printStatus(
        org.paxos.proto.StatusRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPrintStatusMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.SnapshotOffer> getLocalSnapshot(
        com.google.protobuf.Empty request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetLocalSnapshotMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.paxos.proto.Ack> installLocalSnapshot(
        org.paxos.proto.SnapshotOffer request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getInstallLocalSnapshotMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_PREPARE = 0;
  private static final int METHODID_NEW_VIEW = 1;
  private static final int METHODID_ACCEPT = 2;
  private static final int METHODID_COMMIT = 3;
  private static final int METHODID_CATCHUP = 4;
  private static final int METHODID_HEARTBEAT = 5;
  private static final int METHODID_CONTROL = 6;
  private static final int METHODID_SUBMIT = 7;
  private static final int METHODID_GET_BALANCE = 8;
  private static final int METHODID_TWO_PC_PREPARE = 9;
  private static final int METHODID_TWO_PC_COMMIT = 10;
  private static final int METHODID_TWO_PC_ABORT = 11;
  private static final int METHODID_RESET = 12;
  private static final int METHODID_PRINT_LOG = 13;
  private static final int METHODID_PRINT_DB = 14;
  private static final int METHODID_PRINT_VIEW = 15;
  private static final int METHODID_PRINT_STATUS = 16;
  private static final int METHODID_GET_LOCAL_SNAPSHOT = 17;
  private static final int METHODID_INSTALL_LOCAL_SNAPSHOT = 18;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_PREPARE:
          serviceImpl.prepare((org.paxos.proto.PrepareMsg) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.PromiseMsg>) responseObserver);
          break;
        case METHODID_NEW_VIEW:
          serviceImpl.newView((org.paxos.proto.NewViewMsg) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.NewViewAck>) responseObserver);
          break;
        case METHODID_ACCEPT:
          serviceImpl.accept((org.paxos.proto.AcceptMsg) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.AcceptedMsg>) responseObserver);
          break;
        case METHODID_COMMIT:
          serviceImpl.commit((org.paxos.proto.CommitMsg) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.Ack>) responseObserver);
          break;
        case METHODID_CATCHUP:
          serviceImpl.catchup((org.paxos.proto.CatchupRequest) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.CatchupReply>) responseObserver);
          break;
        case METHODID_HEARTBEAT:
          serviceImpl.heartbeat((org.paxos.proto.HeartbeatMsg) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.Ack>) responseObserver);
          break;
        case METHODID_CONTROL:
          serviceImpl.control((org.paxos.proto.NodeControl) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.Ack>) responseObserver);
          break;
        case METHODID_SUBMIT:
          serviceImpl.submit((org.paxos.proto.ClientRequest) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.ClientReply>) responseObserver);
          break;
        case METHODID_GET_BALANCE:
          serviceImpl.getBalance((org.paxos.proto.BalanceRequest) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.BalanceReply>) responseObserver);
          break;
        case METHODID_TWO_PC_PREPARE:
          serviceImpl.twoPcPrepare((org.paxos.proto.TwoPcPrepareMsg) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.TwoPcPreparedReply>) responseObserver);
          break;
        case METHODID_TWO_PC_COMMIT:
          serviceImpl.twoPcCommit((org.paxos.proto.TwoPcCommitMsg) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.Ack>) responseObserver);
          break;
        case METHODID_TWO_PC_ABORT:
          serviceImpl.twoPcAbort((org.paxos.proto.TwoPcAbortMsg) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.Ack>) responseObserver);
          break;
        case METHODID_RESET:
          serviceImpl.reset((com.google.protobuf.Empty) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.Ack>) responseObserver);
          break;
        case METHODID_PRINT_LOG:
          serviceImpl.printLog((com.google.protobuf.Empty) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.LogDump>) responseObserver);
          break;
        case METHODID_PRINT_DB:
          serviceImpl.printDB((com.google.protobuf.Empty) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.DBDump>) responseObserver);
          break;
        case METHODID_PRINT_VIEW:
          serviceImpl.printView((com.google.protobuf.Empty) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.PrintViewDump>) responseObserver);
          break;
        case METHODID_PRINT_STATUS:
          serviceImpl.printStatus((org.paxos.proto.StatusRequest) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.StatusDump>) responseObserver);
          break;
        case METHODID_GET_LOCAL_SNAPSHOT:
          serviceImpl.getLocalSnapshot((com.google.protobuf.Empty) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.SnapshotOffer>) responseObserver);
          break;
        case METHODID_INSTALL_LOCAL_SNAPSHOT:
          serviceImpl.installLocalSnapshot((org.paxos.proto.SnapshotOffer) request,
              (io.grpc.stub.StreamObserver<org.paxos.proto.Ack>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getPrepareMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.paxos.proto.PrepareMsg,
              org.paxos.proto.PromiseMsg>(
                service, METHODID_PREPARE)))
        .addMethod(
          getNewViewMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.paxos.proto.NewViewMsg,
              org.paxos.proto.NewViewAck>(
                service, METHODID_NEW_VIEW)))
        .addMethod(
          getAcceptMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.paxos.proto.AcceptMsg,
              org.paxos.proto.AcceptedMsg>(
                service, METHODID_ACCEPT)))
        .addMethod(
          getCommitMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.paxos.proto.CommitMsg,
              org.paxos.proto.Ack>(
                service, METHODID_COMMIT)))
        .addMethod(
          getCatchupMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.paxos.proto.CatchupRequest,
              org.paxos.proto.CatchupReply>(
                service, METHODID_CATCHUP)))
        .addMethod(
          getHeartbeatMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.paxos.proto.HeartbeatMsg,
              org.paxos.proto.Ack>(
                service, METHODID_HEARTBEAT)))
        .addMethod(
          getControlMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.paxos.proto.NodeControl,
              org.paxos.proto.Ack>(
                service, METHODID_CONTROL)))
        .addMethod(
          getSubmitMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.paxos.proto.ClientRequest,
              org.paxos.proto.ClientReply>(
                service, METHODID_SUBMIT)))
        .addMethod(
          getGetBalanceMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.paxos.proto.BalanceRequest,
              org.paxos.proto.BalanceReply>(
                service, METHODID_GET_BALANCE)))
        .addMethod(
          getTwoPcPrepareMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.paxos.proto.TwoPcPrepareMsg,
              org.paxos.proto.TwoPcPreparedReply>(
                service, METHODID_TWO_PC_PREPARE)))
        .addMethod(
          getTwoPcCommitMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.paxos.proto.TwoPcCommitMsg,
              org.paxos.proto.Ack>(
                service, METHODID_TWO_PC_COMMIT)))
        .addMethod(
          getTwoPcAbortMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.paxos.proto.TwoPcAbortMsg,
              org.paxos.proto.Ack>(
                service, METHODID_TWO_PC_ABORT)))
        .addMethod(
          getResetMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.protobuf.Empty,
              org.paxos.proto.Ack>(
                service, METHODID_RESET)))
        .addMethod(
          getPrintLogMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.protobuf.Empty,
              org.paxos.proto.LogDump>(
                service, METHODID_PRINT_LOG)))
        .addMethod(
          getPrintDBMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.protobuf.Empty,
              org.paxos.proto.DBDump>(
                service, METHODID_PRINT_DB)))
        .addMethod(
          getPrintViewMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.protobuf.Empty,
              org.paxos.proto.PrintViewDump>(
                service, METHODID_PRINT_VIEW)))
        .addMethod(
          getPrintStatusMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.paxos.proto.StatusRequest,
              org.paxos.proto.StatusDump>(
                service, METHODID_PRINT_STATUS)))
        .addMethod(
          getGetLocalSnapshotMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.google.protobuf.Empty,
              org.paxos.proto.SnapshotOffer>(
                service, METHODID_GET_LOCAL_SNAPSHOT)))
        .addMethod(
          getInstallLocalSnapshotMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              org.paxos.proto.SnapshotOffer,
              org.paxos.proto.Ack>(
                service, METHODID_INSTALL_LOCAL_SNAPSHOT)))
        .build();
  }

  private static abstract class NodeServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    NodeServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.paxos.proto.PaxosProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("NodeService");
    }
  }

  private static final class NodeServiceFileDescriptorSupplier
      extends NodeServiceBaseDescriptorSupplier {
    NodeServiceFileDescriptorSupplier() {}
  }

  private static final class NodeServiceMethodDescriptorSupplier
      extends NodeServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    NodeServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (NodeServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new NodeServiceFileDescriptorSupplier())
              .addMethod(getPrepareMethod())
              .addMethod(getNewViewMethod())
              .addMethod(getAcceptMethod())
              .addMethod(getCommitMethod())
              .addMethod(getCatchupMethod())
              .addMethod(getHeartbeatMethod())
              .addMethod(getControlMethod())
              .addMethod(getSubmitMethod())
              .addMethod(getGetBalanceMethod())
              .addMethod(getTwoPcPrepareMethod())
              .addMethod(getTwoPcCommitMethod())
              .addMethod(getTwoPcAbortMethod())
              .addMethod(getResetMethod())
              .addMethod(getPrintLogMethod())
              .addMethod(getPrintDBMethod())
              .addMethod(getPrintViewMethod())
              .addMethod(getPrintStatusMethod())
              .addMethod(getGetLocalSnapshotMethod())
              .addMethod(getInstallLocalSnapshotMethod())
              .build();
        }
      }
    }
    return result;
  }
}
