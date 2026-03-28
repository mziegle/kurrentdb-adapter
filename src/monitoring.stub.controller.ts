import { Controller } from '@nestjs/common';
import { Observable } from 'rxjs';
import {
  MonitoringController,
  MonitoringControllerMethods,
  StatsReq,
  StatsResp,
} from './interfaces/monitoring';
import { createStatsStream } from './stub-utils';

@Controller()
@MonitoringControllerMethods()
export class MonitoringStubController implements MonitoringController {
  stats(request: StatsReq): Observable<StatsResp> {
    void request;
    return createStatsStream();
  }
}
