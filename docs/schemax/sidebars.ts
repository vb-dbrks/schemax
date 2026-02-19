import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

/**
 * Sidebar split: "For Users" (get started, use SchemaX) vs "For Contributors" (develop, test, contribute).
 * Keeps user docs upfront and contributor docs in a collapsed section to reduce cognitive load.
 */
const sidebars: SidebarsConfig = {
  docs: [
    'intro',
    {
      type: 'category',
      label: 'For Users',
      collapsed: false,
      link: { type: 'doc', id: 'guide/quickstart' },
      items: [
        'guide/prerequisites',
        'guide/quickstart',
        'guide/setup',
        'guide/authentication',
        { type: 'doc', id: 'guide/environments-and-scope', label: 'Environments and scope (Advanced)' },
        { type: 'doc', id: 'guide/unity-catalog-grants', label: 'Unity Catalog grants (Advanced)' },
        { type: 'doc', id: 'guide/architecture', label: 'Architecture (Advanced)' },
        'guide/git-and-cicd-setup',
        { type: 'doc', id: 'reference/workflows', label: 'Workflows (Advanced)' },
        { type: 'doc', id: 'reference/cli', label: 'CLI Reference (Advanced)' },
        'reference/faq',
      ],
    },
    {
      type: 'category',
      label: 'For Contributors',
      collapsed: true,
      items: [
        'guide/development',
        'reference/testing',
        'reference/test-coverage-checklist',
        'reference/provider-contract',
        'contributing',
      ],
    },
  ],
};

export default sidebars;
