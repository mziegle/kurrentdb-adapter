export function printOutput(payload: unknown, asJson: boolean): void {
  if (asJson) {
    console.log(JSON.stringify(payload, null, 2));
    return;
  }

  if (typeof payload === 'string') {
    console.log(payload);
    return;
  }

  console.dir(payload, { depth: null, colors: true });
}
