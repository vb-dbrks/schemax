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
        'guide/quickstart',
        'guide/environments-and-scope',
        'guide/unity-catalog-grants',
        'guide/architecture',
        'reference/workflows',
        'reference/cli',
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
