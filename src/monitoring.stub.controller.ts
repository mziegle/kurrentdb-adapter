import { Controller } from '@nestjs/common';
import { from, Observable } from 'rxjs';
import {
  MonitoringController,
  MonitoringControllerMethods,
  StatsReq,
  StatsResp,
} from './interfaces/monitoring';
import { logHotPath } from './debug-log';
import { AdapterStatsService } from './operations/adapter-stats.service';

@Controller()
@MonitoringControllerMethods()
export class MonitoringStubController implements MonitoringController {
  constructor(private readonly statsService: AdapterStatsService) {}

  stats(request: StatsReq): Observable<StatsResp> {
    void request;
    logHotPath('gRPC Monitoring.Stats');
    return from(this.statsService.createGrpcStats());
  }
}
