import Layout from '@theme/Layout';
import useBaseUrl from '@docusaurus/useBaseUrl';
import React from 'react';
import Button from '../components/Button';

const capabilities = [
  {
    title: 'Visual schema designer',
    description:
      'Design catalogs, schemas, tables, and governance in the VS Code extension with inline editing and real-time SQL.',
  },
  {
    title: 'Snapshot-based versioning',
    description:
      'Version your schema as snapshots; replay changelog ops for current state. Semantic versions and rebase support.',
  },
  {
    title: 'Multi-environment & CI/CD',
    description:
      'Environment-specific catalog mapping, SQL generation per target, and apply/rollback with deployment tracking.',
  },
  {
    title: 'Unity Catalog support',
    description:
      'Catalogs, schemas, tables, views, volumes, functions, materialized views; grants, tags, row filters, column masks.',
  },
  {
    title: 'Governance-only mode',
    description:
      'Add comments, tags, grants, row filters, and column masks to existing objects without managing CREATE TABLE.',
  },
  {
    title: 'Python SDK & CLI',
    description:
      'Automate with schemax sql, schemax apply, rollback, snapshot create/validate/rebase, and diff.',
  },
];

function Hero(): React.ReactElement {
  return (
    <div className="landing-hero">
      <img
        src={useBaseUrl('/img/schemax_logov2.svg')}
        alt="SchemaX"
        className="landing-hero__logo"
      />
      <h1 className="landing-hero__title">SchemaX</h1>
      <p className="landing-hero__tagline" style={{ marginBottom: '0.5rem' }}>
        Open source · Git- and CI/CD-friendly
      </p>
      <p className="landing-hero__tagline">
        Modern, low-touch schema migration and management for data catalogs.
        Full or governance-only. Python SDK · VS Code extension.
      </p>
      <div className="landing-hero__links">
        <Button
          link="/docs/guide/quickstart/"
          label="Quickstart"
          variant="secondary"
          outline
          size="large"
        />
        <Button
          link="/docs/guide/prerequisites/"
          label="Prerequisites"
          variant="secondary"
          outline
        />
        <Button
          link="/docs/guide/setup/"
          label="Setup"
          variant="secondary"
          outline
        />
        <Button
          link="/docs/reference/cli/"
          label="CLI Reference"
          variant="secondary"
          outline
        />
        <Button
          link="/docs/guide/architecture/"
          label="Architecture"
          variant="secondary"
          outline
        />
        <Button
          link="/docs/reference/faq/"
          label="FAQ"
          variant="secondary"
          outline
        />
      </div>
    </div>
  );
}

function Capabilities(): React.ReactElement {
  return (
    <div className="landing-capabilities">
      <h2 className="landing-capabilities__title">Capabilities</h2>
      <div className="landing-capabilities__grid">
        {capabilities.map((cap, i) => (
          <div key={i} className="landing-capability-card">
            <div className="landing-capability-card__title">{cap.title}</div>
            <p className="landing-capability-card__desc">{cap.description}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

function CallToAction(): React.ReactElement {
  return (
    <div className="landing-cta">
      <h2 className="landing-cta__title">Get started with SchemaX</h2>
      <p className="landing-cta__text">
        Follow the quickstart to install the extension and CLI, then design and
        apply your first schema.
      </p>
      <Button
        link="/docs/guide/quickstart/"
        label="Start using SchemaX"
        variant="primary"
        size="large"
        className="landing-cta__button"
      />
    </div>
  );
}

export default function Home(): React.ReactElement {
  return (
    <Layout>
      <main>
        <div style={{ maxWidth: 1000, margin: '0 auto' }}>
          <Hero />
          <Capabilities />
          <CallToAction />
        </div>
      </main>
    </Layout>
  );
}
