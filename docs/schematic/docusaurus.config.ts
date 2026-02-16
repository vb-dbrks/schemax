import { themes as prismThemes } from 'prism-react-renderer';
import type { Config } from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const config: Config = {
  title: 'Schematic',
  tagline:
    'Multi-provider data catalog schema management with version control for Unity Catalog and more.',
  favicon: 'img/favicon.svg',

  // CI sets these for GitHub Pages; fallbacks for local build
  url: process.env.GITHUB_PAGES_URL || 'https://github.com',
  baseUrl: process.env.GITHUB_PAGES_BASE_URL || '/schemax-vscode/',
  trailingSlash: true,

  organizationName: process.env.GITHUB_REPOSITORY_OWNER || 'your-org',
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
          editUrl: 'https://github.com/your-org/schematic/tree/main/docs/schematic/',
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
      title: 'Schematic',
      logo: {
        alt: 'Schematic',
        src: 'img/logo.svg',
      },
      items: [
        {
          type: 'search',
          position: 'right',
        },
        {
          href: 'https://github.com/your-org/schematic',
          position: 'right',
          className: 'header-github-link',
          'aria-label': 'GitHub repository',
        },
      ],
    },
    footer: {
      links: [],
      copyright: `Copyright © ${new Date().getFullYear()} Schematic. Docs built with Docusaurus.`,
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
