import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';
import {themes as prismThemes, type PrismTheme} from 'prism-react-renderer';

// Light code theme with every token at WCAG AA (>= 4.5:1) against its background.
// The stock light themes (github, oneLight) use pale comment/number/keyword colors.
const lightCodeTheme: PrismTheme = {
  plain: {color: '#1f2328', backgroundColor: '#f6f8fa'},
  styles: [
    {types: ['comment', 'prolog', 'doctype', 'cdata'], style: {color: '#57606a', fontStyle: 'italic'}},
    {types: ['punctuation', 'operator'], style: {color: '#1f2328'}},
    {types: ['string', 'char', 'attr-value', 'template-string'], style: {color: '#0a3069'}},
    {types: ['keyword', 'atrule', 'selector', 'tag', 'important'], style: {color: '#cf222e'}},
    {types: ['function', 'function-variable', 'class-name', 'builtin'], style: {color: '#8250df'}},
    {
      types: ['number', 'boolean', 'constant', 'variable', 'property', 'attr-name', 'symbol', 'entity', 'url', 'regex'],
      style: {color: '#0550ae'},
    },
    {types: ['inserted'], style: {color: '#116329'}},
    {types: ['deleted'], style: {color: '#82071e'}},
  ],
};

const config: Config = {
  title: 'Orisun',
  tagline: 'Decisions that stay correct as facts change',
  favicon: 'img/favicon.svg',
  url: 'https://orisunlabs.github.io',
  baseUrl: '/Orisun/',
  organizationName: 'OrisunLabs',
  projectName: 'Orisun',
  onBrokenLinks: 'throw',
  trailingSlash: false,

  headTags: [
    {
      tagName: 'link',
      attributes: {rel: 'preconnect', href: 'https://api.fontshare.com'},
    },
    {
      tagName: 'link',
      attributes: {rel: 'dns-prefetch', href: 'https://api.fontshare.com'},
    },
    {
      tagName: 'link',
      attributes: {
        rel: 'stylesheet',
        href: 'https://api.fontshare.com/v2/css?f[]=satoshi@400,500,600,700,800&display=swap',
      },
    },
    {
      tagName: 'link',
      attributes: {rel: 'preconnect', href: 'https://fonts.googleapis.com'},
    },
    {
      tagName: 'link',
      attributes: {rel: 'preconnect', href: 'https://fonts.gstatic.com', crossorigin: 'anonymous'},
    },
    {
      tagName: 'link',
      attributes: {
        rel: 'stylesheet',
        href: 'https://fonts.googleapis.com/css2?family=JetBrains+Mono:ital,wght@0,400..700;1,400..700&display=swap',
      },
    },
  ],

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  plugins: [
    [
      '@docusaurus/plugin-client-redirects',
      {
        redirects: [
          {from: '/docs/start-here', to: '/docs'},
          {from: '/benchmarks', to: '/docs/operations/benchmarks'},
        ],
      },
    ],
  ],

  presets: [
    [
      'classic',
      {
        docs: {
          sidebarPath: './sidebars.ts',
          routeBasePath: '/docs',
          editUrl: 'https://github.com/OrisunLabs/Orisun/tree/main/docs/',
          // The newest cut version serves at /docs; in-progress docs for the
          // next release live at /docs/next with an "unreleased" banner.
          lastVersion: '0.14.3',
          versions: {
            current: {
              label: 'Next 🚧',
              banner: 'unreleased',
            },
          },
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
      } satisfies Preset.Options,
    ],
  ],

  themeConfig: {
    image: 'img/orisun-social.png',
    colorMode: {
      respectPrefersColorScheme: true,
    },
    docs: {
      sidebar: {
        hideable: true,
        autoCollapseCategories: false,
      },
    },
    metadata: [
      {
        name: 'description',
        content:
          'Orisun is an event database for decisions that must stay correct as facts change, with declared context checks and ordered backend subscriptions.',
      },
    ],
    navbar: {
      title: 'Orisun',
      logo: {
        alt: 'Orisun',
        src: 'img/logo.svg',
        srcDark: 'img/logo-dark.svg',
      },
      items: [
        {to: '/docs', label: 'Docs', position: 'left'},
        {to: '/docs/tutorial', label: 'Tutorial', position: 'left'},
        {to: '/docs/concepts', label: 'Concepts', position: 'left'},
        {to: '/docs/api', label: 'API', position: 'left'},
        {to: '/docs/operations', label: 'Operations', position: 'left'},
        {type: 'docsVersionDropdown', position: 'right'},
        {href: 'https://github.com/OrisunLabs/Orisun/releases', label: 'Releases', position: 'right'},
        {href: 'https://hub.docker.com/repository/docker/orisunlabs/orisun', label: 'Docker', position: 'right'},
        {href: 'https://github.com/OrisunLabs/Orisun', label: 'GitHub', position: 'right'},
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Docs',
          items: [
            {label: 'Overview', to: '/docs'},
            {label: 'Getting Started', to: '/docs/getting-started'},
            {label: 'Tutorial', to: '/docs/tutorial'},
            {label: 'Concepts', to: '/docs/concepts'},
            {label: 'EventStore API', to: '/docs/api/eventstore'},
            {label: 'Embedding', to: '/docs/embedding/go'},
          ],
        },
        {
          title: 'Operations',
          items: [
            {label: 'Configuration', to: '/docs/operations/configuration'},
            {label: 'Security', to: '/docs/operations/security'},
            {label: 'Deployment', to: '/docs/operations/deployment'},
            {label: 'Observability', to: '/docs/operations/observability'},
            {label: 'Benchmarks', to: '/docs/operations/benchmarks'},
            {label: 'Troubleshooting', to: '/docs/operations/troubleshooting'},
          ],
        },
        {
          title: 'Project',
          items: [
            {label: 'GitHub', href: 'https://github.com/OrisunLabs/Orisun'},
            {label: 'Releases', href: 'https://github.com/OrisunLabs/Orisun/releases'},
            {label: 'Docker Hub', href: 'https://hub.docker.com/repository/docker/orisunlabs/orisun'},
          ],
        },
      ],
      copyright: `Copyright ${new Date().getFullYear()} Orisun contributors. Released under the MIT License.`,
    },
    prism: {
      theme: lightCodeTheme,
      darkTheme: prismThemes.vsDark,
      additionalLanguages: ['bash', 'go', 'java', 'json', 'typescript', 'yaml', 'protobuf'],
    },
    tableOfContents: {
      minHeadingLevel: 2,
      maxHeadingLevel: 3,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
