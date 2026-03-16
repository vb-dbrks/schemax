 Plan: Naming Standards for Database Objects            

 Context

 Data platform teams need consistent naming conventions enforced at the point of creation in SchemaX. Currently there is no naming
  standards feature. The user wants the feature built Python-first: all validation logic (regex matching, suggestion generation)
 lives in the Python SDK/CLI, and the VS Code webview UI is a pure display layer that delegates validation to Python via the
 existing extension host message protocol.

 ---
 Architecture

 Webview (UI only)
   ↓ postMessage("validate-name", {name, objectType})
 Extension Host (extension.ts)
   ↓ pythonBackendClient.run(["validate-name", ...])
 Python CLI (schemax validate-name)
   ↓ reads project.json namingStandards config, validates, returns suggestion
   ↑ JSON CommandEnvelope { valid, error, suggestion, pattern, description }
 Extension Host
   ↑ postMessage("name-validation-result", result)
 Webview
   → blocks ADD / shows warning modal for RENAME

 Existing pattern from run-import → import-result is the exact model to follow.

 ---
 Phase 1: Python SDK/CLI

 1a. Data model — packages/python-sdk/src/schemax/core/naming.py (NEW)

 New module for all naming-standards business logic:

 @dataclass(slots=True, frozen=True)
 class NamingRule:
     pattern: str          # regex, e.g. "^[a-z][a-z0-9_]*$"
     enabled: bool
     description: str = ""
     examples_valid: list[str] = field(default_factory=list)
     examples_invalid: list[str] = field(default_factory=list)

 @dataclass(slots=True, frozen=True)
 class NamingStandardsConfig:
     apply_to_renames: bool = False
     catalog: NamingRule | None = None
     schema: NamingRule | None = None
     table: NamingRule | None = None
     view: NamingRule | None = None
     column: NamingRule | None = None

     @classmethod
     def from_dict(cls, d: dict[str, Any]) -> "NamingStandardsConfig": ...

 Key functions in this module:
 - validate_name(name, rule) -> tuple[bool, str | None] — (is_valid, error_message_or_None)
 - suggest_name(name, pattern) -> str — sanitisation: lowercase if pattern has no uppercase range, replace -, ., spaces with _,
 strip remaining invalid chars, collapse __
 - validate_naming_standards(state, config) -> list[str] — iterates all catalog objects, returns list of violation strings for CLI
  reporting

 1b. New CLI command — schemax validate-name

 In packages/python-sdk/src/schemax/cli.py, add:

 schemax validate-name --name <name> --type <object_type> [workspace]

 - --type: one of catalog, schema, table, view, column
 - workspace: path to .schemax/ project (defaults to .)
 - Output is always JSON CommandEnvelope (matches existing envelope format)

 data payload shape:
 {
   "valid": true|false,
   "name": "MyTable",
   "objectType": "table",
   "error": "Name does not match naming standard (Lowercase snake_case: ^[a-z][a-z0-9_]*$)",
   "suggestion": "my_table",
   "pattern": "^[a-z][a-z0-9_]*$",
   "description": "Lowercase snake_case"
 }

 If no naming standard is configured for that object type (or the standard is disabled), returns { "valid": true, ... } — no-op.

 Implementation: new validate_name_command.py in packages/python-sdk/src/schemax/commands/, using
 NamingStandardsConfig.from_dict(project["settings"].get("namingStandards", {})).

 1c. Enhance schemax validate

 In packages/python-sdk/src/schemax/commands/validate.py, call validate_naming_standards(state, config) after the existing
 dependency-validation step. Append violations to the warnings list (soft — validate still exits 0 but surfaces them). This
 satisfies acceptance criterion 6.

 1d. DEFAULT_PROJECT_SETTINGS — no change

 namingStandards is optional. When absent, validation returns valid: true immediately.

 ---
 Phase 2: TypeScript Storage Types

 packages/vscode-extension/src/storage-v4.ts

 Add to ProjectSettings:

 export interface NamingRule {
   pattern: string;
   enabled: boolean;
   description?: string;
   examples?: { valid: string[]; invalid: string[] };
 }

 export interface NamingStandardsConfig {
   applyToRenames: boolean;
   catalog?: NamingRule;
   schema?: NamingRule;
   table?: NamingRule;
   view?: NamingRule;
   column?: NamingRule;
 }

 interface ProjectSettings {
   autoIncrementVersion: boolean;
   versionPrefix: string;
   namingStandards?: NamingStandardsConfig;  // new — optional
 }

 ---
 Phase 3: Extension Host Bridge

 packages/vscode-extension/src/extension.ts

 Add new message handler alongside existing cases (line ~1874):

 case "validate-name": {
   const { name, objectType } = message.payload as { name: string; objectType: string };
   const workspacePath = storageV4.getWorkspacePath();
   const result = await pythonClient.run(
     ["validate-name", "--name", name, "--type", objectType, "--json", workspacePath],
     workspacePath
   );
   panel.webview.postMessage({
     type: "name-validation-result",
     payload: result.envelope?.data ?? { valid: true }
   });
   break;
 }

 No new contracts needed — uses the existing PythonBackendClient.run() and CommandEnvelope types.

 ---
 Phase 4: Webview UI

 4a. New component — NamingStandardsSettings.tsx

 Location: packages/vscode-extension/src/webview/components/settings/NamingStandardsSettings.tsx

 Props: config: NamingStandardsConfig | undefined, onChange: (c: NamingStandardsConfig) => void

 UI:
 - Global applyToRenames checkbox at top
 - "Load Template" button → opens a simple inline panel (not a separate modal) showing 4 preset templates (static TypeScript
 constants — just UI helpers to fill in the form, no Python required):
   - Databricks Best Practices: ^[a-z][a-z0-9_]*$ — lowercase snake_case, all types
   - Data Warehouse Patterns: tables ^(dim_|fact_|stg_|int_)[a-z0-9_]+$, others snake_case
   - camelCase: ^[a-z][a-zA-Z0-9]*$
   - PascalCase: ^[A-Z][a-zA-Z0-9]*$
 - Per-type rows for Catalog, Schema, Table, View, Column — each has: enabled checkbox, pattern text input, description input,
 valid/invalid examples inputs
 - Styles: add .naming-standards-* classes to styles.css using existing tokens

 4b. ProjectSettingsPanel.tsx

 Insert <NamingStandardsSettings> just before the "Physical Isolation (Managed Tables)" settings-section block (line ~273). Wire
 onChange to update editedProject.settings.namingStandards via setEditedProject + setIsDirty(true).

 4c. Async validation hook — utils/useNameValidation.ts (NEW)

 // Returns a function that calls validate-name via extension and returns the result
 export function useNameValidation() {
   const [pending, setPending] = useState(false);

   const validate = useCallback(
     async (name: string, objectType: string): Promise<NameValidationResult> => {
       setPending(true);
       return new Promise((resolve) => {
         vscode.postMessage({ type: "validate-name", payload: { name, objectType } });
         const handler = (event: MessageEvent) => {
           if (event.data.type === "name-validation-result") {
             window.removeEventListener("message", handler);
             setPending(false);
             resolve(event.data.payload);
           }
         };
         window.addEventListener("message", handler);
       });
     },
     []
   );

   return { validate, pending };
 }

 4d. ADD handlers — strict enforcement

 In each add-confirm handler, after validateUnityCatalogObjectName() passes:
 1. Disable confirm button + show brief spinner (pending from hook)
 2. Call validate(name, objectType)
 3. If result.valid === false → set error state with result.error (which already includes the suggestion from Python)
 4. If valid → proceed with store action

 Components: Sidebar.tsx (catalog, schema, table, view add), ColumnGrid.tsx (add column).

 4e. RENAME handlers — soft warning

 Only fires when applyToRenames: true. Flow:
 1. User submits rename modal
 2. Call validate(name, objectType)
 3. If invalid → show <NamingWarningModal> (new small component):
   - Message: Python's result.error
   - Suggestion displayed (clickable → auto-fills rename input)
   - Buttons: "Rename Anyway" (proceed) | "Go Back" (dismiss warning, return to rename input)
 4. If valid → proceed normally

 For column inline rename (ColumnGrid.tsx): on Enter/blur, if applyToRenames: true, run async check. While checking, keep the
 input in edit mode. If violation → open warning modal. If valid → commit rename.

 Components: Sidebar.tsx, TableDesigner.tsx, CatalogDetails.tsx, SchemaDetails.tsx, ColumnGrid.tsx.

 NamingWarningModal can be a shared component at components/NamingWarningModal.tsx since it's used by multiple components.

 ---
 Critical Files

 ┌───────────────────────────────────────────────────────────────────────────────────────┬─────────────────────────────────────┐
 │                                         File                                          │               Change                │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │                                                                                       │ New — NamingRule,                   │
 │ packages/python-sdk/src/schemax/core/naming.py                                        │ NamingStandardsConfig,              │
 │                                                                                       │ validate_name(), suggest_name(),    │
 │                                                                                       │ validate_naming_standards()         │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │ packages/python-sdk/src/schemax/commands/validate_name.py                             │ New — validate_name_command()       │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │ packages/python-sdk/src/schemax/cli.py                                                │ Register validate-name command      │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │ packages/python-sdk/src/schemax/commands/validate.py                                  │ Call validate_naming_standards(),   │
 │                                                                                       │ append to warnings                  │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │                                                                                       │ Add NamingRule,                     │
 │ packages/vscode-extension/src/storage-v4.ts                                           │ NamingStandardsConfig to            │
 │                                                                                       │ ProjectSettings                     │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │ packages/vscode-extension/src/extension.ts                                            │ Handle "validate-name" message,     │
 │                                                                                       │ call Python, relay result           │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │ packages/vscode-extension/src/webview/utils/useNameValidation.ts                      │ New — async validation hook         │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │ packages/vscode-extension/src/webview/components/settings/NamingStandardsSettings.tsx │ New — settings UI                   │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │ packages/vscode-extension/src/webview/components/NamingWarningModal.tsx               │ New — shared rename warning modal   │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │ packages/vscode-extension/src/webview/components/ProjectSettingsPanel.tsx             │ Embed <NamingStandardsSettings>     │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │ packages/vscode-extension/src/webview/components/Sidebar.tsx                          │ Async strict check in add handlers  │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │ packages/vscode-extension/src/webview/components/TableDesigner.tsx                    │ Async soft warning in rename        │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │ packages/vscode-extension/src/webview/components/ColumnGrid.tsx                       │ Async strict/soft in add/rename     │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │ packages/vscode-extension/src/webview/components/CatalogDetails.tsx                   │ Async soft warning in rename        │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │ packages/vscode-extension/src/webview/components/SchemaDetails.tsx                    │ Async soft warning in rename        │
 ├───────────────────────────────────────────────────────────────────────────────────────┼─────────────────────────────────────┤
 │ packages/vscode-extension/src/webview/styles.css                                      │ .naming-standards-* styles          │
 └───────────────────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────┘

 ---
 Verification

 1. Python unit tests (pytest tests/ -q): add tests in packages/python-sdk/tests/unit/test_naming.py covering validate_name(),
 suggest_name(), from_dict().
 2. CLI smoke test: schemax validate-name --name "MyTable" --type table --json . on a project with ^[a-z][a-z0-9_]*$ table
 standard → returns valid: false, suggestion: "my_table". With no standard configured → returns valid: true.
 3. Settings UI: Open "View Project Settings" → "Naming Standards" section appears above "Physical Isolation". Load "Databricks
 Best Practices" template, save → project.json contains settings.namingStandards.
 4. ADD strict: Configure table standard ^[a-z][a-z0-9_]*$. Try adding table MyTable → blocked with Python's error message +
 suggestion.
 5. RENAME soft (applyToRenames: true): Rename table to MyTable → warning modal appears with suggestion. "Rename Anyway" proceeds.
  "Go Back" returns to rename input.
 6. RENAME exempt (applyToRenames: false): Same rename → no modal, proceeds directly.
 7. CLI validate: schemax validate --json . on project with violations → warnings[] contains naming violation strings.
 8. Extension Jest tests: npm test in packages/vscode-extension — all pass.