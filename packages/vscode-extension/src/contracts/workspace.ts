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
