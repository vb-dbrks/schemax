import React from 'react';
import { VSCodeButton } from '@vscode/webview-ui-toolkit/react';

interface NamingWarningModalProps {
  /** The full error message from Python (includes pattern info). */
  error: string;
  /** Suggested replacement name, or null if none. */
  suggestion: string | null;
  /** Called when the user accepts the suggestion; receives the suggested name. */
  onUseSuggestion: (name: string) => void;
  /** Called when the user chooses to rename anyway (proceed with the original name). */
  onProceed: () => void;
  /** Called when the user chooses to go back (dismiss). */
  onCancel: () => void;
}

/**
 * Soft-warning modal shown when a rename/add violates the configured naming
 * standard and ``applyToRenames`` is true. The user can accept the Python
 * suggestion, proceed anyway, or cancel.
 */
export const NamingWarningModal: React.FC<NamingWarningModalProps> = ({
  error,
  suggestion,
  onUseSuggestion,
  onProceed,
  onCancel,
}) => {
  return (
    <div className="modal" role="alertdialog" aria-modal="true" onClick={onCancel}>
      <div
        className="modal-content modal-surface naming-warning-modal"
        onClick={(e) => e.stopPropagation()}
      >
        <h3>Naming Standard Violation</h3>
        <p className="naming-warning-modal__message">{error}</p>
        {suggestion && (
          <div className="naming-warning-modal__suggestion">
            <span>Suggested name:</span>
            <button
              className="naming-warning-modal__suggestion-btn"
              onClick={() => onUseSuggestion(suggestion)}
              title="Use this suggestion"
            >
              {suggestion}
            </button>
          </div>
        )}
        <div className="modal-buttons">
          <VSCodeButton appearance="secondary" onClick={onCancel}>
            Go Back
          </VSCodeButton>
          <VSCodeButton appearance="secondary" onClick={onProceed}>
            Rename Anyway
          </VSCodeButton>
        </div>
      </div>
    </div>
  );
};
