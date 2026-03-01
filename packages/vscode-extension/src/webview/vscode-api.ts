// VS Code API singleton to prevent multiple acquisitions.
// Keep this boundary permissive because extension/webview payloads vary by command.
interface VsCodeApiBridge {
  postMessage: (message: unknown) => void;
  getState: () => unknown;
  setState: (state: unknown) => void;
}

declare const acquireVsCodeApi: () => {
  postMessage: (message: unknown) => void;
  getState: () => unknown;
  setState: (state: unknown) => void;
};

type VsCodeApi = ReturnType<typeof acquireVsCodeApi>;

let vsCodeApi: VsCodeApi | null = null;

export function getVsCodeApi(): VsCodeApi {
  if (vsCodeApi) {
    return vsCodeApi;
  }

  // Store in window to persist across hot reloads
  const windowWithBridge = window as Window & { vsCodeApi?: VsCodeApi };
  if (typeof windowWithBridge.vsCodeApi !== 'undefined') {
    vsCodeApi = windowWithBridge.vsCodeApi;
    return vsCodeApi!;
  }

  try {
    if (typeof acquireVsCodeApi !== 'undefined') {
      vsCodeApi = acquireVsCodeApi();
      windowWithBridge.vsCodeApi = vsCodeApi;
      console.warn('[SchemaX] VS Code API acquired successfully');
      return vsCodeApi;
    }
  } catch (error: unknown) {
    console.warn('[SchemaX] Could not acquire VS Code API:', error);
  }

  // Fallback mock for development
  const fallbackApi: VsCodeApiBridge = {
    postMessage: (message: unknown) => console.warn('[Mock] postMessage:', message),
    getState: () => ({}),
    setState: (state: unknown) => console.warn('[Mock] setState:', state),
  };

  vsCodeApi = fallbackApi as VsCodeApi;
  windowWithBridge.vsCodeApi = vsCodeApi;
  return vsCodeApi;
}
