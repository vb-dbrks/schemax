/**
 * Provider-neutral workspace contracts shared across extension runtime and webview.
 */

export interface Operation {
  id: string;
  ts: string;
  provider: string;
  op: string;
  target: string;
  payload: Record<string, unknown>;
  target_name?: string | null; // v5 multi-target: which target this op belongs to
}

export interface ProviderCapabilities {
  supportedOperations: string[];
  supportedObjectTypes: string[];
  hierarchy: {
    levels?: Array<{
      name?: string;
      displayName?: string;
      pluralName?: string;
      icon?: string;
      isContainer?: boolean;
    }>;
  };
  features: Record<string, boolean>;
}
