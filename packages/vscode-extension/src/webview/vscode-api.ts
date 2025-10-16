// VS Code API singleton to prevent multiple acquisitions
declare const acquireVsCodeApi: () => {
  postMessage: (message: any) => void;
  getState: () => any;
  setState: (state: any) => void;
};

type VsCodeApi = ReturnType<typeof acquireVsCodeApi>;

let vsCodeApi: VsCodeApi | null = null;

export function getVsCodeApi(): VsCodeApi {
  if (vsCodeApi) {
    return vsCodeApi;
  }

  // Store in window to persist across hot reloads
  if (typeof (window as any).vsCodeApi !== 'undefined') {
    vsCodeApi = (window as any).vsCodeApi;
    return vsCodeApi!;
  }

  try {
    if (typeof acquireVsCodeApi !== 'undefined') {
      vsCodeApi = acquireVsCodeApi();
      (window as any).vsCodeApi = vsCodeApi;
      console.log('[Schematic] VS Code API acquired successfully');
      return vsCodeApi;
    }
  } catch (error) {
    console.warn('[Schematic] Could not acquire VS Code API:', error);
  }

  // Fallback mock for development
  vsCodeApi = {
    postMessage: (message: any) => console.log('[Mock] postMessage:', message),
    getState: () => ({}),
    setState: (state: any) => console.log('[Mock] setState:', state),
  };
  
  (window as any).vsCodeApi = vsCodeApi;
  return vsCodeApi;
}

