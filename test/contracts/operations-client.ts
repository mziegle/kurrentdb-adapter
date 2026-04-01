import {
  credentials,
  loadPackageDefinition,
  ServiceError,
} from '@grpc/grpc-js';
import { loadSync } from '@grpc/proto-loader';
import { join } from 'node:path';
import { ProtoGrpcType } from '../../generated/grpc/operations';
import { OperationsClient } from '../../generated/grpc/event_store/client/operations/Operations';
import { ScavengeResp__Output } from '../../generated/grpc/event_store/client/operations/ScavengeResp';
import { StartScavengeReq } from '../../generated/grpc/event_store/client/operations/StartScavengeReq';

export type OperationsScavengeResponse = ScavengeResp__Output;

const OPERATIONS_PROTO_PATH = join(
  __dirname,
  '../../proto/Grpc/operations.proto',
);
const PROTO_INCLUDE_DIR = join(__dirname, '../../proto/Grpc');

let operationsClientConstructor:
  | ProtoGrpcType['event_store']['client']['operations']['Operations']
  | undefined;

function getOperationsClientConstructor(): ProtoGrpcType['event_store']['client']['operations']['Operations'] {
  if (!operationsClientConstructor) {
    const packageDefinition = loadSync(OPERATIONS_PROTO_PATH, {
      keepCase: true,
      longs: Number,
      defaults: true,
      oneofs: true,
      includeDirs: [PROTO_INCLUDE_DIR],
    });
    const proto = loadPackageDefinition(
      packageDefinition,
    ) as unknown as ProtoGrpcType;

    operationsClientConstructor =
      proto.event_store.client.operations.Operations;
  }

  return operationsClientConstructor;
}

export function createOperationsClient(grpcAddress: string): OperationsClient {
  const Client = getOperationsClientConstructor();
  return new Client(grpcAddress, credentials.createInsecure());
}

export function createStartScavengeRequest(): StartScavengeReq {
  return {
    options: {
      thread_count: 1,
      start_from_chunk: 0,
    },
  };
}

export function unaryCall<TResponse>(
  invoke: (
    callback: (error: ServiceError | null, response?: TResponse) => void,
  ) => void,
): Promise<TResponse> {
  return new Promise<TResponse>((resolve, reject) => {
    invoke((error, response) => {
      if (error) {
        reject(error);
        return;
      }

      if (response === undefined) {
        reject(new Error('Expected unary gRPC response.'));
        return;
      }

      resolve(response);
    });
  });
}

export function mapScavengeResponse(response: ScavengeResp__Output): {
  scavengeId: string;
  scavengeResult: number;
} {
  return {
    scavengeId: response.scavenge_id,
    scavengeResult: response.scavenge_result,
  };
}
