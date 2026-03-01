import type { RuntimeInfoData } from './contracts';

export interface CompatibilityResult {
  ok: boolean;
  reason?: string;
}

function parseSemver(version: string): [number, number, number] | null {
  const match = version.trim().match(/^v?(\d+)\.(\d+)\.(\d+)$/);
  if (!match) {
    return null;
  }
  return [Number(match[1]), Number(match[2]), Number(match[3])];
}

function compareSemver(left: string, right: string): number {
  const leftParts = parseSemver(left);
  const rightParts = parseSemver(right);
  if (!leftParts || !rightParts) {
    return 0;
  }
  for (let index = 0; index < 3; index += 1) {
    const lVal = leftParts[index];
    const rVal = rightParts[index];
    if (lVal > rVal) {
      return 1;
    }
    if (lVal < rVal) {
      return -1;
    }
  }
  return 0;
}

export function validateRuntimeInfo(
  runtimeInfo: RuntimeInfoData,
  minCliVersion: string,
  requiredEnvelopeSchemaVersion: string
): CompatibilityResult {
  if (runtimeInfo.envelopeSchemaVersion !== requiredEnvelopeSchemaVersion) {
    return {
      ok: false,
      reason:
        `Unsupported envelope schema ${runtimeInfo.envelopeSchemaVersion}; ` +
        `required ${requiredEnvelopeSchemaVersion}.`,
    };
  }
  if (compareSemver(runtimeInfo.cliVersion, minCliVersion) < 0) {
    return {
      ok: false,
      reason: `SchemaX CLI ${runtimeInfo.cliVersion} is older than required ${minCliVersion}.`,
    };
  }
  return { ok: true };
}
