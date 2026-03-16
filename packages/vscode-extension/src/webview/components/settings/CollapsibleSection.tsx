import React, { useState } from "react";

interface CollapsibleSectionProps {
  title: string;
  children: React.ReactNode;
  defaultOpen?: boolean;
  isOpen?: boolean;
  onToggle?: () => void;
}

export function CollapsibleSection({
  title,
  children,
  defaultOpen = true,
  isOpen,
  onToggle,
}: CollapsibleSectionProps): React.ReactElement {
  const [internalOpen, setInternalOpen] = useState(defaultOpen);
  const open = isOpen !== undefined ? isOpen : internalOpen;
  const handleToggle = onToggle ?? (() => setInternalOpen((v) => !v));

  return (
    <div className="collapsible-section">
      <button
        type="button"
        className="collapsible-section__header"
        onClick={handleToggle}
        aria-expanded={open}
        aria-controls={`collapsible-${title.replace(/\s+/g, "-")}`}
      >
        <i
          className={`codicon ${open ? "codicon-chevron-down" : "codicon-chevron-right"}`}
          aria-hidden="true"
        />
        <h3 className="collapsible-section__title">{title}</h3>
      </button>
      {open && (
        <div id={`collapsible-${title.replace(/\s+/g, "-")}`} className="collapsible-section__body">
          {children}
        </div>
      )}
    </div>
  );
}
