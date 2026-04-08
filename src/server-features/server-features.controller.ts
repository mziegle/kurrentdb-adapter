import { Controller } from '@nestjs/common';
import { GrpcMethod } from '@nestjs/microservices';
import {
  SupportedMethods,
  SupportedMethod,
} from '../interfaces/serverfeatures';
import { logHotPath } from '../debug-log';

const GOSSIP_SERVICE_NAME = 'event_store.client.gossip.gossip';
const MONITORING_SERVICE_NAME = 'event_store.client.monitoring.monitoring';
const OPERATIONS_SERVICE_NAME = 'event_store.client.operations.operations';
const PERSISTENT_SUBSCRIPTIONS_SERVICE_NAME =
  'event_store.client.persistent_subscriptions.persistentsubscriptions';
const SERVER_FEATURES_SERVICE_NAME =
  'event_store.client.server_features.serverfeatures';
const STREAMS_SERVICE_NAME = 'event_store.client.streams.streams';
const USERS_SERVICE_NAME = 'event_store.client.users.users';

@Controller()
export class ServerFeaturesController {
  @GrpcMethod('ServerFeatures', 'getSupportedMethods')
  getSupportedMethods(): SupportedMethods {
    const response = {
      eventStoreServerVersion: '24.0.0',
      methods: [
        this.createSupportedMethod('append', STREAMS_SERVICE_NAME),
        this.createSupportedMethod('batchappend', STREAMS_SERVICE_NAME, [
          'deadline_duration',
        ]),
        this.createSupportedMethod('delete', STREAMS_SERVICE_NAME),
        this.createSupportedMethod('read', STREAMS_SERVICE_NAME, [
          'position',
          'events',
        ]),
        this.createSupportedMethod('tombstone', STREAMS_SERVICE_NAME),
        this.createSupportedMethod(
          'create',
          PERSISTENT_SUBSCRIPTIONS_SERVICE_NAME,
          ['stream', 'all'],
        ),
        this.createSupportedMethod(
          'delete',
          PERSISTENT_SUBSCRIPTIONS_SERVICE_NAME,
          ['stream', 'all'],
        ),
        this.createSupportedMethod(
          'getinfo',
          PERSISTENT_SUBSCRIPTIONS_SERVICE_NAME,
          ['stream', 'all'],
        ),
        this.createSupportedMethod(
          'list',
          PERSISTENT_SUBSCRIPTIONS_SERVICE_NAME,
          ['stream', 'all'],
        ),
        this.createSupportedMethod(
          'read',
          PERSISTENT_SUBSCRIPTIONS_SERVICE_NAME,
          ['stream', 'all'],
        ),
        this.createSupportedMethod(
          'replayparked',
          PERSISTENT_SUBSCRIPTIONS_SERVICE_NAME,
          ['stream', 'all'],
        ),
        this.createSupportedMethod(
          'restartsubsystem',
          PERSISTENT_SUBSCRIPTIONS_SERVICE_NAME,
          ['stream', 'all'],
        ),
        this.createSupportedMethod(
          'update',
          PERSISTENT_SUBSCRIPTIONS_SERVICE_NAME,
          ['stream', 'all'],
        ),
        this.createSupportedMethod('mergeindexes', OPERATIONS_SERVICE_NAME),
        this.createSupportedMethod(
          'restartpersistentsubscriptions',
          OPERATIONS_SERVICE_NAME,
        ),
        this.createSupportedMethod('resignnode', OPERATIONS_SERVICE_NAME),
        this.createSupportedMethod('setnodepriority', OPERATIONS_SERVICE_NAME),
        this.createSupportedMethod('shutdown', OPERATIONS_SERVICE_NAME),
        this.createSupportedMethod('startscavenge', OPERATIONS_SERVICE_NAME),
        this.createSupportedMethod('stopscavenge', OPERATIONS_SERVICE_NAME),
        this.createSupportedMethod('read', GOSSIP_SERVICE_NAME),
        this.createSupportedMethod('stats', MONITORING_SERVICE_NAME),
        this.createSupportedMethod('changepassword', USERS_SERVICE_NAME),
        this.createSupportedMethod('create', USERS_SERVICE_NAME),
        this.createSupportedMethod('delete', USERS_SERVICE_NAME),
        this.createSupportedMethod('details', USERS_SERVICE_NAME),
        this.createSupportedMethod('disable', USERS_SERVICE_NAME),
        this.createSupportedMethod('enable', USERS_SERVICE_NAME),
        this.createSupportedMethod('resetpassword', USERS_SERVICE_NAME),
        this.createSupportedMethod('update', USERS_SERVICE_NAME),
        this.createSupportedMethod(
          'getsupportedmethods',
          SERVER_FEATURES_SERVICE_NAME,
        ),
      ],
    };

    logHotPath('gRPC ServerFeatures.GetSupportedMethods', {
      detail: `version=${response.eventStoreServerVersion} methods=${response.methods.length}`,
    });
    return response;
  }

  private createSupportedMethod(
    methodName: string,
    serviceName: string,
    features: string[] = [],
  ): SupportedMethod {
    return {
      methodName,
      serviceName,
      features,
    };
  }
}
