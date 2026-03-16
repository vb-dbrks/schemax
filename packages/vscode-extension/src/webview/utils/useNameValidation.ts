import { useState, useCallback } from "react";
import { getVsCodeApi } from "../vscode-api";

export interface NameValidationResult {
  valid: boolean;
  name: string;
  objectType: string;
  error: string | null;
  suggestion: string | null;
  pattern: string | null;
  description: string | null;
}

const vscode = getVsCodeApi();

export function useNameValidation(): {
  validate: (name: string, objectType: string) => Promise<NameValidationResult>;
  pending: boolean;
} {
  const [pending, setPending] = useState(false);

  const validate = useCallback(
    (name: string, objectType: string): Promise<NameValidationResult> => {
      setPending(true);
      return new Promise<NameValidationResult>((resolve) => {
        const handler = (event: MessageEvent) => {
          if (event.data?.type === "name-validation-result") {
            window.removeEventListener("message", handler);
            setPending(false);
            resolve(event.data.payload as NameValidationResult);
          }
        };
        window.addEventListener("message", handler);
        vscode.postMessage({ type: "validate-name", payload: { name, objectType } });
      });
    },
    []
  );

  return { validate, pending };
}
