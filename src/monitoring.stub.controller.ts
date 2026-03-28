import { Controller } from '@nestjs/common';
import { Observable } from 'rxjs';
import {
  MonitoringController,
  MonitoringControllerMethods,
  StatsReq,
  StatsResp,
} from './interfaces/monitoring';
import { createStatsStream } from './stub-utils';
import { logHotPath } from './debug-log';

@Controller()
@MonitoringControllerMethods()
export class MonitoringStubController implements MonitoringController {
  stats(request: StatsReq): Observable<StatsResp> {
    void request;
    logHotPath('gRPC Monitoring.Stats');
    return createStatsStream();
  }
}
