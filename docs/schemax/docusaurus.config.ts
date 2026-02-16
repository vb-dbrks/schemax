import { themes as prismThemes } from 'prism-react-renderer';
import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'SchemaX',
  tagline:
    'Open source: multi-provider data catalog schema management with version control. Python SDK on PyPI · VS Code extension on Marketplace.',
  favicon: 'img/schemax_favicon.svg',

  // CI sets these for GitHub Pages; fallbacks for local build
  url: process.env.GITHUB_PAGES_URL || 'https://github.com',
  baseUrl: process.env.GITHUB_PAGES_BASE_URL || '/schemax-vscode/',
  trailingSlash: true,

  organizationName: process.env.GITHUB_REPOSITORY_OWNER || 'vb-dbrks',
  projectName: process.env.GITHUB_REPOSITORY?.split('/')[1] || 'schemax-vscode',

  onBrokenLinks: 'throw',
  onDuplicateRoutes: 'throw',
  onBrokenAnchors: 'warn',
  markdown: {
    mermaid: true,
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  plugins: [
    'docusaurus-plugin-image-zoom',
    'docusaurus-lunr-search',
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          routeBasePath: '/',
          sidebarPath: './sidebars.ts',
          editUrl: 'https://github.com/vb-dbrks/schemax/tree/main/docs/schemax/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    colorMode: {
      defaultMode: 'dark',
      respectPrefersColorScheme: false,
    },
    navbar: {
      title: 'SchemaX',
      logo: {
        alt: 'SchemaX',
        src: 'img/schemax_favicon.svg',
      },
      items: [
        {
          type: 'search',
          position: 'right',
        },
        {
          href: `https://github.com/${process.env.GITHUB_REPOSITORY_OWNER || 'vb-dbrks'}/schemax-vscode`,
          position: 'right',
          className: 'header-github-link',
          'aria-label': 'GitHub repository',
        },
      ],
    },
    footer: {
      links: [
        {
          title: 'Distribution',
          items: [
            {
              label: 'Python SDK (PyPI)',
              href: 'https://pypi.org/project/schemaxpy/',
            },
            {
              label: 'VS Code Extension (Marketplace)',
              href: 'https://marketplace.visualstudio.com/items?itemName=varunbhandary.schemax-vscode',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/vb-dbrks/schemax-vscode',
            },
          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} SchemaX. Open source. Docs built with Docusaurus.`,
    },
    prism: {
      theme: prismThemes.oneLight,
      darkTheme: prismThemes.oneDark,
    },
    zoom: {
      selector: 'article img',
      background: {
        light: '#F8FAFC',
        dark: '#F8FAFC',
      },
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
