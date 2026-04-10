function jsonReplacer(_key: string, value: unknown): unknown {
  if (typeof value === 'bigint') {
    return value.toString();
  }

  return value;
}

export function stringifyCliJson(payload: unknown): string {
  return JSON.stringify(payload, jsonReplacer, 2);
}

export function printOutput(payload: unknown, asJson: boolean): void {
  if (asJson) {
    console.log(stringifyCliJson(payload));
    return;
  }

  if (typeof payload === 'string') {
    console.log(payload);
    return;
  }

  console.dir(payload, { depth: null, colors: true });
}
