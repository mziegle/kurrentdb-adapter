import { Controller } from '@nestjs/common';
import { from, Observable } from 'rxjs';
import {
  MonitoringController as MonitoringControllerContract,
  MonitoringControllerMethods,
  StatsReq,
  StatsResp,
} from '../interfaces/monitoring';
import { logHotPath } from '../shared/debug-log';
import { AdapterStatsService } from '../operations/adapter-stats.service';

@Controller()
@MonitoringControllerMethods()
export class MonitoringController implements MonitoringControllerContract {
  constructor(private readonly statsService: AdapterStatsService) {}

  stats(request: StatsReq): Observable<StatsResp> {
    void request;
    logHotPath('gRPC Monitoring.Stats');
    return from(this.statsService.createGrpcStats());
  }
}
