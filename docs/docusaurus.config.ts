import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

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
        href: 'https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;500;650&display=swap',
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
          lastVersion: '0.10.0',
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
          'Orisun is an event database for decisions that must stay correct as facts change, with declared context checks and sequential publishing per boundary.',
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
      additionalLanguages: ['bash', 'go', 'java', 'json', 'typescript', 'yaml', 'protobuf'],
    },
    tableOfContents: {
      minHeadingLevel: 2,
      maxHeadingLevel: 3,
    },
  } satisfies Preset.ThemeConfig,
};

export default config;
