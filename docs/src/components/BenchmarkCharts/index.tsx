import React, {useId, useMemo, useState} from 'react';

import styles from './styles.module.css';

const clients = [1, 4, 16, 64, 256] as const;

const backends = [
  {key: 'postgres', label: 'PostgreSQL', color: 'var(--benchmark-postgres)'},
  {key: 'sqlite', label: 'SQLite', color: 'var(--benchmark-sqlite)'},
  {
    key: 'foundationdb',
    label: 'FoundationDB',
    color: 'var(--benchmark-foundationdb)',
  },
] as const;

type BackendKey = (typeof backends)[number]['key'];
type Series = Record<BackendKey, readonly number[]>;
type Workload = {
  label: string;
  shortLabel: string;
  throughput: Series;
  p99: Series;
};

const workloads: Record<string, Workload> = {
  'ccc-1': {
    label: 'CCC append, 1 event',
    shortLabel: 'CCC append · 1 event',
    throughput: {
      postgres: [507.057, 1078.29, 3600.419, 9495.733, 16987.907],
      sqlite: [2479.517, 6296.322, 12930.323, 17165.543, 18542.239],
      foundationdb: [186.232, 697.483, 2023.832, 3920.136, 5578.222],
    },
    p99: {
      postgres: [5.397, 9.484, 11.212, 15.516, 34.342],
      sqlite: [1.171, 2.252, 3.612, 10.924, 40.419],
      foundationdb: [16.797, 16.101, 22.241, 53.926, 219.642],
    },
  },
  'ccc-100': {
    label: 'CCC append, 100 events',
    shortLabel: 'CCC append · 100 events',
    throughput: {
      postgres: [17996.59, 26546.417, 35906.252, 25352.725, 28479.044],
      sqlite: [38996.561, 57389.554, 60761.534, 55135.686, 57532.875],
      foundationdb: [8473.317, 14266.569, 20012.981, 20322.803, 18502.629],
    },
    p99: {
      postgres: [13.631, 42.422, 89.547, 498.837, 1323.518],
      sqlite: [8.158, 18.798, 56.144, 240.365, 653.447],
      foundationdb: [55.127, 115.158, 302.967, 1436.641, 3271.168],
    },
  },
  'unconditional-1': {
    label: 'Unconditional append, 1 event',
    shortLabel: 'Unconditional append',
    throughput: {
      postgres: [494.496, 1062.736, 3685.721, 10747.267, 19888.892],
      sqlite: [2064.827, 6688.316, 13978.502, 22837.898, 27279.773],
      foundationdb: [159.598, 719.923, 1946.447, 3895.923, 4973.278],
    },
    p99: {
      postgres: [10.131, 12.669, 14.109, 15.79, 34.191],
      sqlite: [2.233, 1.965, 4.414, 8.844, 24.108],
      foundationdb: [36.093, 18.025, 30.723, 68.581, 313.913],
    },
  },
  'conditional-read': {
    label: 'Conditional read',
    shortLabel: 'Conditional read',
    throughput: {
      postgres: [4553.311, 14707.896, 34931.156, 48902.254, 52509.941],
      sqlite: [10109.636, 26105.268, 77263.054, 91564.157, 98985.075],
      foundationdb: [973.326, 3944.973, 9804.822, 14785.347, 20421.476],
    },
    p99: {
      postgres: [7.354, 7.883, 14.837, 44.517, 208.295],
      sqlite: [7.822, 8.877, 7.727, 16.429, 81.359],
      foundationdb: [30.19, 21.239, 49.828, 124.749, 278.627],
    },
  },
  'projection-read': {
    label: 'Projection read',
    shortLabel: 'Projection read',
    throughput: {
      postgres: [133332.882, 261598.492, 371555.933, 356194.84, 342457.467],
      sqlite: [82999.818, 303732.639, 712450.512, 765185.758, 767918.573],
      foundationdb: [49899.824, 143197.416, 235730.481, 260228.181, 264529.288],
    },
    p99: {
      postgres: [21.895, 45.032, 120.113, 379.093, 2946.76],
      sqlite: [43.09, 39.307, 54.851, 146.624, 599.552],
      foundationdb: [52.616, 101.209, 203.865, 486.885, 1696.57],
    },
  },
  mixed: {
    label: 'Append with four readers',
    shortLabel: 'Append + 4 readers',
    throughput: {
      postgres: [253.133, 569.73, 2072.369, 6151.144, 12897.376],
      sqlite: [1448.192, 4407.705, 8848.472, 12329.853, 13427.223],
      foundationdb: [118.2, 503.697, 1575.884, 3954.684, 6302.919],
    },
    p99: {
      postgres: [12.957, 20.894, 21.763, 28.204, 52.552],
      sqlite: [5.178, 4.812, 7.251, 20.164, 49.076],
      foundationdb: [21.63, 17.386, 26.431, 44.31, 98.578],
    },
  },
};

const chartWidth = 680;
const chartHeight = 300;
const padding = {top: 22, right: 18, bottom: 46, left: 72};
const plotWidth = chartWidth - padding.left - padding.right;
const plotHeight = chartHeight - padding.top - padding.bottom;

function niceMaximum(value: number): number {
  const exponent = Math.floor(Math.log10(value));
  const magnitude = 10 ** exponent;
  const normalized = value / magnitude;
  const nice =
    normalized <= 1 ? 1 : normalized <= 2 ? 2 : normalized <= 5 ? 5 : 10;
  return nice * magnitude;
}

function formatAxis(value: number): string {
  if (value >= 1_000_000) return `${value / 1_000_000}m`;
  if (value >= 1_000) return `${value / 1_000}k`;
  return String(Math.round(value));
}

function formatExact(value: number): string {
  return new Intl.NumberFormat('en-US', {
    maximumFractionDigits: 2,
  }).format(value);
}

type ChartProps = {
  id: string;
  metric: 'throughput' | 'p99';
  title: string;
  workload: Workload;
};

function Chart({id, metric, title, workload}: ChartProps) {
  const [activePoint, setActivePoint] = useState<string | null>(null);
  const series = workload[metric];
  const maxValue = niceMaximum(
    Math.max(...backends.flatMap((backend) => series[backend.key])),
  );
  const ticks = [0, 0.25, 0.5, 0.75, 1].map(
    (fraction) => maxValue * fraction,
  );
  const x = (index: number) =>
    padding.left + (plotWidth * index) / (clients.length - 1);
  const y = (value: number) =>
    padding.top + plotHeight - (value / maxValue) * plotHeight;
  const unit = metric === 'throughput' ? 'events/s' : 'ms';

  return (
    <section className={styles.chartCard}>
      <div className={styles.chartHeading}>
        <h3>{title}</h3>
        <span>{unit}</span>
      </div>
      <svg
        className={styles.chart}
        viewBox={`0 0 ${chartWidth} ${chartHeight}`}
        role="img"
        aria-labelledby={`${id}-title ${id}-description`}>
        <title id={`${id}-title`}>
          {title} for {workload.label}
        </title>
        <desc id={`${id}-description`}>
          Comparison of PostgreSQL, SQLite, and FoundationDB at 1, 4, 16, 64,
          and 256 clients.
        </desc>

        {ticks.map((tick) => (
          <g key={tick}>
            <line
              className={styles.gridLine}
              x1={padding.left}
              x2={chartWidth - padding.right}
              y1={y(tick)}
              y2={y(tick)}
            />
            <text
              className={styles.axisText}
              x={padding.left - 12}
              y={y(tick) + 4}
              textAnchor="end">
              {formatAxis(tick)}
            </text>
          </g>
        ))}

        <line
          className={styles.axisLine}
          x1={padding.left}
          x2={chartWidth - padding.right}
          y1={padding.top + plotHeight}
          y2={padding.top + plotHeight}
        />

        {clients.map((client, index) => (
          <g key={client}>
            <line
              className={styles.tickLine}
              x1={x(index)}
              x2={x(index)}
              y1={padding.top + plotHeight}
              y2={padding.top + plotHeight + 6}
            />
            <text
              className={styles.axisText}
              x={x(index)}
              y={chartHeight - 18}
              textAnchor="middle">
              {client}
            </text>
          </g>
        ))}

        <text
          className={styles.axisLabel}
          x={padding.left + plotWidth / 2}
          y={chartHeight - 2}
          textAnchor="middle">
          concurrent clients
        </text>

        {backends.map((backend) => {
          const values = series[backend.key];
          const path = values
            .map(
              (value, index) =>
                `${index === 0 ? 'M' : 'L'} ${x(index)} ${y(value)}`,
            )
            .join(' ');

          return (
            <g key={backend.key}>
              <path
                className={styles.seriesLine}
                d={path}
                stroke={backend.color}
              />
              {values.map((value, index) => {
                const description = `${backend.label} · ${clients[index]} clients · ${formatExact(value)} ${unit}`;
                return (
                  <circle
                    key={clients[index]}
                    className={styles.dataPoint}
                    cx={x(index)}
                    cy={y(value)}
                    r="4.5"
                    fill={backend.color}
                    tabIndex={0}
                    aria-label={description}
                    onFocus={() => setActivePoint(description)}
                    onBlur={() => setActivePoint(null)}
                    onMouseEnter={() => setActivePoint(description)}
                    onMouseLeave={() => setActivePoint(null)}>
                    <title>{description}</title>
                  </circle>
                );
              })}
            </g>
          );
        })}
      </svg>
      <p className={styles.readout} aria-live="polite">
        {activePoint ?? 'Hover or focus a point for its exact value.'}
      </p>
    </section>
  );
}

export default function BenchmarkCharts() {
  const [workloadKey, setWorkloadKey] = useState('ccc-1');
  const id = useId().replace(/:/g, '');
  const workload = workloads[workloadKey];
  const workloadOptions = useMemo(() => Object.entries(workloads), []);

  return (
    <div className={styles.root}>
      <div
        className={styles.workloadPicker}
        role="group"
        aria-label="Benchmark workload">
        {workloadOptions.map(([key, option]) => (
          <button
            key={key}
            type="button"
            className={key === workloadKey ? styles.activeButton : styles.button}
            aria-pressed={key === workloadKey}
            onClick={() => setWorkloadKey(key)}>
            {option.shortLabel}
          </button>
        ))}
      </div>

      <div className={styles.legend} aria-label="Database legend">
        {backends.map((backend) => (
          <span key={backend.key}>
            <i style={{backgroundColor: backend.color}} />
            {backend.label}
          </span>
        ))}
      </div>

      <div className={styles.chartGrid}>
        <Chart
          id={`${id}-throughput`}
          metric="throughput"
          title="Throughput"
          workload={workload}
        />
        <Chart
          id={`${id}-latency`}
          metric="p99"
          title="p99 latency"
          workload={workload}
        />
      </div>
    </div>
  );
}
