import { EventEmitter } from 'node:events';
import { WriteRequestContext } from '../event-store/event-store-backend';

type GrpcCall = EventEmitter & {
  cancelled: boolean;
  destroyed?: boolean;
  getDeadline(): Date | number;
};

/** The caller must dispose after the backend settles, not at input half-close. */
export function createWriteContext(
  call: GrpcCall,
  groupDeadline?: number,
  closeCancels = true,
): {
  context: WriteRequestContext;
  dispose: () => void;
} {
  const controller = new AbortController();
  const deadline = Math.min(
    Number(call.getDeadline()),
    groupDeadline ?? Infinity,
  );
  const context: WriteRequestContext = { signal: controller.signal };
  if (Number.isFinite(deadline)) context.deadline = deadline;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const dispose = () => {
    if (timer) clearTimeout(timer);
    call.off('cancelled', abort);
    call.off('close', abort);
    call.off('error', abort);
  };
  const abort = () => {
    controller.abort();
    dispose();
  };
  const schedule = () => {
    if (controller.signal.aborted || context.deadline === undefined) return;
    const remaining = context.deadline - Date.now();
    if (remaining <= 0) abort();
    else timer = setTimeout(schedule, Math.min(remaining, 2147483647));
  };
  call.on('cancelled', abort);
  if (closeCancels) call.on('close', abort);
  call.on('error', abort);
  if (call.cancelled || call.destroyed) abort();
  schedule();
  return {
    context,
    dispose,
  };
}
