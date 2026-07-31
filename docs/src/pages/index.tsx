import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';
import Layout from '@theme/Layout';
import clsx from 'clsx';
import type {ReactNode} from 'react';
import styles from './index.module.css';

type LinkItem = readonly [title: string, href: string, description: string];
type FlowStep = readonly [number: string, title: string, description: string];

const heroFacts = [
  ['History', 'Complete transactional event log'],
  ['Decisions', 'Declared context checked at commit'],
  ['Delivery', 'Catch-up plus live JetStream'],
];

const outcomes = [
  {
    label: 'Evidence',
    title: 'Preserve how the system got here',
    description:
      'Keep the events behind current state so decisions, projections, audits, and future models share the same durable source of truth.',
  },
  {
    label: 'Validation',
    title: 'Refuse decisions made on changed facts',
    description:
      'A command declares the events it relied on. Orisun checks that same subset inside the write transaction and rejects a stale save.',
  },
  {
    label: 'Scope',
    title: 'Match the real business invariant',
    description:
      'Define consistency with event-content queries across the relevant entities instead of forcing every rule into one fixed stream.',
  },
  {
    label: 'Reaction',
    title: 'Recover and continue from every commit',
    description:
      'Durable checkpoints provide no-skip, sequential publishing per boundary. Subscribers catch up from storage before moving live.',
  },
];

const useCases = [
  {
    title: 'Payments and ledgers',
    tension: 'Two transfers are each valid against the same balance, but not together.',
    context: 'Validate the source balance and both account histories before committing both legs atomically.',
  },
  {
    title: 'Inventory and reservations',
    tension: 'Concurrent requests act on availability that another request has already consumed.',
    context: 'Scope the write to the product, reservation, and allocation events that determined availability.',
  },
  {
    title: 'Workflows and approvals',
    tension: 'A step is approved after policy, permissions, or an earlier decision has changed.',
    context: 'Check the workflow and policy events the command evaluated before recording its outcome.',
  },
  {
    title: 'Agent actions',
    tension: 'An agent proposes an action from facts that changed while it was reasoning or waiting on a tool.',
    context: 'Record the relevant facts as events and reject the action when its declared context is no longer current.',
  },
];

const flow: FlowStep[] = [
  ['1', 'Read the evidence', 'Query one consistent snapshot of the event history the command needs.'],
  ['2', 'Make the decision', 'Apply business rules in application code and produce the next events.'],
  ['3', 'Validate and commit', 'Save only if the same declared event subset is still at the expected position.'],
  ['4', 'Deliver the result', 'Publish sequentially per boundary, with storage-backed catch-up before live delivery.'],
];

const backends = [
  {
    name: 'SQLite',
    href: '/docs/getting-started#run-sqlite-from-a-binary',
    summary: 'Single-node production, embedded applications, edge services, and local development.',
    details: ['one active node', 'JSON criteria', 'durable checkpoints'],
  },
  {
    name: 'PostgreSQL',
    href: '/docs/getting-started#run-postgresql-from-a-binary',
    summary: 'Multi-node Orisun deployments with mature database operations and shared storage.',
    details: ['cluster locks', 'schema boundaries', 'PgBouncer'],
  },
  {
    name: 'FoundationDB',
    href: '/docs/operations/foundationdb',
    summary: 'Beta clustered backend with ordered key ranges and parallel commits.',
    details: ['versionstamps', 'fenced leases', 'covering indexes'],
  },
];

const docGroups: {title: string; links: LinkItem[]}[] = [
  {
    title: 'Learn the model',
    links: [
      ['What is Orisun?', '/docs', 'Guarantees, fit, and the shortest path to each task.'],
      ['Command Context Consistency', '/docs/concepts/command-context-consistency', 'The command-first consistency model.'],
    ],
  },
  {
    title: 'Build with it',
    links: [
      ['Tutorial', '/docs/tutorial', 'Build a two-account ledger and live projector.'],
      ['EventStore API', '/docs/api/eventstore', 'Save, query, subscribe, and manage indexes.'],
      ['Admin API', '/docs/api/admin', 'Create, import, and inspect event-backed boundaries.'],
      ['Client libraries', '/docs/api/clients', 'Integrate with Go, Node.js, Java, or grpcurl.'],
    ],
  },
  {
    title: 'Run it',
    links: [
      ['Getting Started', '/docs/getting-started', 'Start a binary, container, or embedded store.'],
      ['Storage backends', '/docs/concepts/storage-backends', 'Choose a deployment profile.'],
      ['Operations', '/docs/operations/deployment', 'Configure, secure, observe, and scale Orisun.'],
    ],
  },
];

const commandExample = `const latest =
  await client.getLatestByCriteria({
    boundary: 'accounts',
    criteria,
  });
const expectedPosition = latest.contextPosition;

await client.saveEvents({
  boundary: 'accounts',
  query: {
    expectedPosition,
    subsetQuery: {criteria},
  },
  events,
});`;

const dockerExample = `docker run --rm \\
  -p 5005:5005 \\
  -e ORISUN_BACKEND=sqlite \\
  -e ORISUN_SQLITE_DIR=/var/lib/orisun/sqlite \\
  -e ORISUN_NATS_CLUSTER_ENABLED=false \\
  -e ORISUN_ADMIN_BOUNDARY=orisun_admin \\
  -v orisun-data:/var/lib/orisun \\
  orisunlabs/orisun:0.6.1-sqlite`;

export default function Home(): ReactNode {
  const diagramUrl = useBaseUrl('/img/orisun-flow.svg');

  return (
    <Layout
      title="The Event Database for Decisions That Must Stay Correct"
      description="Orisun preserves complete event history, validates each declared command context at commit time, and publishes committed events sequentially within each boundary."
    >
      <header className={styles.hero}>
        <div className={styles.heroGrid} />
        <div className={styles.heroTrace} aria-hidden="true">
          <span>40</span><strong>AccountOpened</strong>
          <span>41</span><strong>MoneyCredited</strong>
          <span>42</span><strong>LimitChanged</strong>
          <span>43</span><strong>MoneyDebited</strong>
          <span>44</span><strong>TransferRecorded</strong>
        </div>
        <div className={clsx('container', styles.heroInner)}>
          <div className={styles.badges}>
            <a className={styles.badgeLink} href="https://github.com/OrisunLabs/Orisun/releases/latest">
              Latest release
            </a>
            <span className={styles.badge}>MIT open source</span>
            <span className={styles.badge}>gRPC API</span>
          </div>
          <h1>The event database for decisions that stay correct.</h1>
          <p>
            Orisun preserves complete event history and lets a command declare the facts behind
            its decision. At commit time, Orisun checks that context again. Changed facts reject
            the write; a valid decision commits atomically and moves to catch-up plus live delivery.
          </p>
          <div className={styles.actions}>
            <Link className="button button--primary button--lg" to="/docs/getting-started">
              Start with SQLite
            </Link>
            <Link className="button button--secondary button--lg" to="/docs/tutorial">
              See the ledger tutorial
            </Link>
          </div>
          <dl className={styles.heroFacts}>
            {heroFacts.map(([label, value]) => (
              <div key={label}>
                <dt>{label}</dt>
                <dd>{value}</dd>
              </div>
            ))}
          </dl>
        </div>
      </header>

      <main>
        <section id="stale-decision" className={clsx('section', styles.problemSection)}>
          <div className="container">
            <div className={styles.sectionHeader}>
              <span className={styles.eyebrow}>The stale-decision problem</span>
              <h2>Correct business logic can still commit the wrong result.</h2>
              <p>
                Two commands can read the same valid state and both make a reasonable decision.
                The second decision becomes unsafe when the first command changes the facts it
                relied on. Orisun makes that race part of the write contract.
              </p>
            </div>

            <div className={styles.raceStage} aria-label="Two concurrent transfers against the same account balance">
              <div className={styles.contextBar}>
                <span>Declared context</span>
                <strong>Account balance $100</strong>
                <code>position 42</code>
              </div>
              <div className={styles.raceGrid}>
                <article className={clsx(styles.raceLane, styles.acceptedLane)}>
                  <div className={styles.laneHeader}>
                    <span>Command A</span>
                    <strong>Transfer $80</strong>
                  </div>
                  <div className={styles.lanePath}>
                    <span>Reads $100</span>
                    <i aria-hidden="true" />
                    <span>Decision valid</span>
                    <i aria-hidden="true" />
                    <strong>Committed</strong>
                  </div>
                  <p>Writes at position 43. The account balance is now $20.</p>
                </article>
                <article className={clsx(styles.raceLane, styles.rejectedLane)}>
                  <div className={styles.laneHeader}>
                    <span>Command B</span>
                    <strong>Transfer $60</strong>
                  </div>
                  <div className={styles.lanePath}>
                    <span>Read $100</span>
                    <i aria-hidden="true" />
                    <span>Was valid at 42</span>
                    <i aria-hidden="true" />
                    <strong>Rejected</strong>
                  </div>
                  <p><code>ALREADY_EXISTS</code>: re-read the context and decide again.</p>
                </article>
              </div>
              <div className={styles.raceResult}>
                <span>Without the check</span>
                <strong>Both transfers land and the account reaches -$40.</strong>
                <span>With Orisun</span>
                <strong>The stale second write never commits.</strong>
              </div>
            </div>
          </div>
        </section>

        <section id="outcomes" className="section">
          <div className="container">
            <div className={styles.sectionHeader}>
              <span className={styles.eyebrow}>What changes</span>
              <h2>History becomes an active guardrail, not just an audit trail.</h2>
              <p>
                Orisun closes the loop between the evidence a command reads, the decision in
                application code, the atomic write, and the systems that react afterward.
              </p>
            </div>
            <div className={styles.outcomeGrid}>
              {outcomes.map((outcome) => (
                <article className={styles.outcomeCard} key={outcome.title}>
                  <span>{outcome.label}</span>
                  <h3>{outcome.title}</h3>
                  <p>{outcome.description}</p>
                </article>
              ))}
            </div>
          </div>
        </section>

        <section id="use-cases" className={clsx('section', styles.useCaseSection)}>
          <div className="container">
            <div className={styles.sectionHeader}>
              <span className={styles.eyebrow}>Where it matters</span>
              <h2>Use the facts that make each decision safe.</h2>
              <p>
                The application still owns its domain rules. Orisun gives each command a dynamic,
                content-defined consistency boundary for enforcing them under concurrency.
              </p>
            </div>
            <div className={styles.useCaseGrid}>
              {useCases.map((useCase, index) => (
                <article className={styles.useCaseCard} key={useCase.title}>
                  <span>{String(index + 1).padStart(2, '0')}</span>
                  <h3>{useCase.title}</h3>
                  <p>{useCase.tension}</p>
                  <strong>{useCase.context}</strong>
                </article>
              ))}
            </div>
          </div>
        </section>

        <section id="command-loop" className="section">
          <div className="container">
            <div className={styles.sectionHeader}>
              <span className={styles.eyebrow}>One command loop</span>
              <h2>Read, decide, validate, deliver.</h2>
              <p>
                Business decisions stay in application code. Orisun owns the transactional context
                check and the durable path from a committed event to every recovering subscriber.
              </p>
            </div>
            <div className={styles.flowGrid}>
              {flow.map(([number, title, description]) => (
                <article className={styles.flowStep} key={title}>
                  <strong>{number}</strong>
                  <h3>{title}</h3>
                  <p>{description}</p>
                </article>
              ))}
            </div>
          </div>
        </section>

        <section id="command-context-consistency" className={clsx('section', styles.mechanismSection)}>
          <div className={clsx('container', styles.mechanismGrid)}>
            <div className={styles.mechanismCopy}>
              <span className={styles.eyebrow}>The mechanism</span>
              <h2>Consistency shaped around each command.</h2>
              <p>
                Command Context Consistency lets each decision declare the exact event subset it
                relies on. Query that context, remember its position, then save only if it remains
                unchanged. Define and activate the application boundary through the Admin API
                before entering this command loop.
              </p>
              <div className={styles.mechanismLinks}>
                <Link to="/docs/concepts/command-context-consistency">Understand CCC</Link>
                <Link to="/docs/api/eventstore">Use the EventStore API</Link>
              </div>
            </div>
            <div className={styles.codePanel}>
              <div className={styles.codePanelHeader}>
                <span>Node.js</span>
                <strong>Read and save with the same criteria</strong>
              </div>
              <pre><code>{commandExample}</code></pre>
            </div>
          </div>
        </section>

        <section id="architecture" className="section">
          <div className={clsx('container', styles.architectureGrid)}>
            <div className={styles.architectureMedia}>
              <img src={diagramUrl} alt="Command, Orisun storage, and JetStream delivery flow" />
            </div>
            <div className={styles.architectureCopy}>
              <span className={styles.eyebrow}>Built as one system</span>
              <h2>The durable log and delivery path share one ordering contract.</h2>
              <p>
                Storage remains the source of truth. Embedded JetStream is the live delivery layer,
                while durable publisher checkpoints ensure committed events are not skipped.
                Publishing is at least once and sequential within each boundary.
              </p>
              <ul>
                <li>Transactional event writes and context checks</li>
                <li>Runtime boundary creation through a durable event-backed catalog</li>
                <li>Durable per-boundary publisher checkpoints</li>
                <li>Storage-backed catch-up before live delivery</li>
                <li>gRPC, auth, indexes, telemetry, and admin APIs</li>
              </ul>
              <Link to="/docs/concepts/delivery-guarantees">Read the delivery guarantees</Link>
            </div>
          </div>
        </section>

        <section id="backends" className={clsx('section', styles.backendSection)}>
          <div className="container">
            <div className={styles.sectionHeader}>
              <span className={styles.eyebrow}>Deploy your way</span>
              <h2>Keep the API. Change the operational shape.</h2>
              <p>
                Start with a complete SQLite server, then use PostgreSQL or the FoundationDB
                beta backend when the deployment needs multiple nodes or distributed storage.
              </p>
            </div>
            <div className={styles.backendGrid}>
              {backends.map((backend) => (
                <Link className={styles.backendCard} to={backend.href} key={backend.name}>
                  <div className={styles.backendHeader}>
                    <h3>{backend.name}</h3>
                    <span>Open guide</span>
                  </div>
                  <p>{backend.summary}</p>
                  <ul>
                    {backend.details.map((detail) => <li key={detail}>{detail}</li>)}
                  </ul>
                </Link>
              ))}
            </div>
          </div>
        </section>

        <section id="start" className={clsx('section', styles.startSection)}>
          <div className={clsx('container', styles.startGrid)}>
            <div className={styles.startCopy}>
              <span className={styles.eyebrow}>Start locally</span>
              <h2>A complete event database in one SQLite container.</h2>
              <p>
                Run the event log, context checks, publisher checkpoints, admin state, and embedded
                JetStream together. The same EventStore API carries forward to every backend.
              </p>
              <div className={styles.actions}>
                <Link className="button button--primary button--lg" to="/docs/getting-started">
                  Follow getting started
                </Link>
                <a className="button button--secondary button--lg" href="https://hub.docker.com/repository/docker/orisunlabs/orisun">
                  View Docker images
                </a>
              </div>
            </div>
            <div className={styles.terminal}>
              <div className={styles.terminalHeader}>
                <span>SQLite</span>
                <strong>localhost:5005</strong>
              </div>
              <pre><code>{dockerExample}</code></pre>
            </div>
          </div>
        </section>

        <section id="documentation" className={clsx('section', styles.docsSection)}>
          <div className="container">
            <div className={styles.sectionHeader}>
              <span className={styles.eyebrow}>Documentation</span>
              <h2>Go from the model to a running system.</h2>
            </div>
            <div className={styles.docGroups}>
              {docGroups.map((group) => (
                <section className={styles.docGroup} key={group.title}>
                  <h3>{group.title}</h3>
                  {group.links.map(([title, href, description]) => (
                    <Link className={styles.docLink} to={href} key={href}>
                      <strong>{title}</strong>
                      <span>{description}</span>
                    </Link>
                  ))}
                </section>
              ))}
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}
