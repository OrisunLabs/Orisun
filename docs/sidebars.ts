import type {SidebarsConfig} from '@docusaurus/plugin-content-docs';

const sidebars: SidebarsConfig = {
  docs: [
    {type: 'html', value: 'Start here', className: 'sidebar-section-title sidebar-section-title--first'},
    'overview',
    'getting-started',
    'tutorial',
    'comparison',

    {type: 'html', value: 'Understand', className: 'sidebar-section-title'},
    {
      type: 'category',
      label: 'Concepts',
      collapsed: false,
      link: {
        type: 'generated-index',
        title: 'Concepts',
        slug: '/concepts',
        description:
          'The model behind Orisun: how commands declare context, how positions order the log, and what the store guarantees on every backend.',
      },
      items: [
        'concepts/command-context-consistency',
        'concepts/positions',
        'concepts/storage-backends',
        'concepts/delivery-guarantees',
        'concepts/indexing',
      ],
    },
    'internals',

    {type: 'html', value: 'Build', className: 'sidebar-section-title'},
    {
      type: 'category',
      label: 'API reference',
      collapsed: false,
      link: {
        type: 'generated-index',
        title: 'API reference',
        slug: '/api',
        description:
          'The gRPC surface: save, query, subscribe, and manage indexes with EventStore; manage boundaries and users with Admin; and integrate from Go, Node.js, or Java.',
      },
      items: ['api/eventstore', 'api/admin', 'api/clients'],
    },
    {
      type: 'category',
      label: 'Patterns',
      collapsed: false,
      link: {
        type: 'generated-index',
        title: 'Patterns',
        slug: '/patterns',
        description:
          'Field-tested recipes for building on Orisun: idempotent writes, projection rebuilds, event scoping, and schema evolution.',
      },
      items: [
        'patterns/idempotency-and-retry',
        'patterns/projection-rebuild',
        'patterns/event-scopes',
        'patterns/event-schema-evolution',
      ],
    },
    {
      type: 'category',
      label: 'Embedding',
      collapsed: false,
      link: {
        type: 'generated-index',
        title: 'Embedding',
        slug: '/embedding',
        description: 'Run Orisun inside your own process instead of as a separate server.',
      },
      items: ['embedding/go'],
    },

    {type: 'html', value: 'Operate', className: 'sidebar-section-title'},
    {
      type: 'category',
      label: 'Operations',
      collapsed: false,
      link: {
        type: 'generated-index',
        title: 'Operations',
        slug: '/operations',
        description:
          'Everything for running Orisun in production: configuration, security, deployment topologies, observability, and troubleshooting.',
      },
      items: [
        'operations/upgrading-0.7-to-0.8',
        'operations/configuration',
        'operations/security',
        'operations/deployment',
        'operations/foundationdb',
        'operations/observability',
        'operations/benchmarks',
        'operations/troubleshooting',
      ],
    },

    {type: 'html', value: 'Project', className: 'sidebar-section-title'},
    'project/development',
    'project/releases',
  ],
};

export default sidebars;
