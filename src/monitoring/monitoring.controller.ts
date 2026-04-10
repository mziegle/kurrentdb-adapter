import { Controller } from '@nestjs/common';
import { from, Observable } from 'rxjs';
import {
  MonitoringController as MonitoringControllerContract,
  MonitoringControllerMethods,
  StatsReq,
  StatsResp,
} from '../interfaces/monitoring';
import { Metadata } from '@grpc/grpc-js';
import {
  extractGrpcMetadata,
  logHotPath,
  summarizeGrpcMetadata,
} from '../shared/debug-log';
import { AdapterStatsService } from '../operations/adapter-stats.service';

@Controller()
@MonitoringControllerMethods()
export class MonitoringController implements MonitoringControllerContract {
  constructor(private readonly statsService: AdapterStatsService) {}

  stats(request: StatsReq, metadata?: Metadata): Observable<StatsResp> {
    logHotPath('gRPC Monitoring.Stats', {
      summary: summarizeGrpcMetadata(metadata),
      trace: {
        metadata: extractGrpcMetadata(metadata),
        request,
      },
    });
    return from(this.statsService.createGrpcStats());
  }
}
