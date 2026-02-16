import React from 'react';

interface RichCommentProps {
  text: string;
  collapsedLines?: number;
  collapseThreshold?: number;
}

type Block =
  | { type: 'paragraph'; text: string }
  | { type: 'unordered_list'; items: string[] }
  | { type: 'ordered_list'; items: string[] };

export const RichComment: React.FC<RichCommentProps> = ({
  text,
  collapsedLines = 7,
  collapseThreshold = 360,
}) => {
  const normalized = (text || '').trim();
  const blocks = React.useMemo(() => parseBlocks(normalized), [normalized]);
  const shouldCollapse = normalized.length > collapseThreshold || normalized.split('\n').length > collapsedLines + 1;
  const [expanded, setExpanded] = React.useState<boolean>(false);

  React.useEffect(() => {
    setExpanded(false);
  }, [normalized]);

  if (!normalized) return null;

  return (
    <div className="rich-comment">
      <div
        className={`rich-comment__content ${shouldCollapse && !expanded ? 'is-collapsed' : ''}`}
        style={{ ['--collapsed-lines' as string]: String(collapsedLines) }}
      >
        {blocks.map((block, index) => {
          if (block.type === 'unordered_list') {
            return (
              <ul key={`ul-${index}`} className="rich-comment__list">
                {block.items.map((item, itemIndex) => (
                  <li key={`uli-${itemIndex}`}>{renderInlineMarkdown(item)}</li>
                ))}
              </ul>
            );
          }
          if (block.type === 'ordered_list') {
            return (
              <ol key={`ol-${index}`} className="rich-comment__list rich-comment__list--ordered">
                {block.items.map((item, itemIndex) => (
                  <li key={`oli-${itemIndex}`}>{renderInlineMarkdown(item)}</li>
                ))}
              </ol>
            );
          }
          return (
            <p key={`p-${index}`} className="rich-comment__paragraph">
              {renderInlineMarkdown(block.text)}
            </p>
          );
        })}
      </div>
      {shouldCollapse && (
        <button
          type="button"
          className="rich-comment__toggle"
          onClick={() => setExpanded((current) => !current)}
        >
          {expanded ? 'Show less' : 'Show more'}
        </button>
      )}
    </div>
  );
};

function parseBlocks(raw: string): Block[] {
  const lines = raw.split('\n');
  const blocks: Block[] = [];
  let i = 0;

  while (i < lines.length) {
    const current = lines[i].trim();
    if (!current) {
      i += 1;
      continue;
    }

    if (/^[-*+]\s+/.test(current)) {
      const items: string[] = [];
      while (i < lines.length) {
        const line = lines[i].trim();
        const match = line.match(/^[-*+]\s+(.+)$/);
        if (!match) break;
        items.push(match[1].trim());
        i += 1;
      }
      blocks.push({ type: 'unordered_list', items });
      continue;
    }

    if (/^\d+\.\s+/.test(current)) {
      const items: string[] = [];
      while (i < lines.length) {
        const line = lines[i].trim();
        const match = line.match(/^\d+\.\s+(.+)$/);
        if (!match) break;
        items.push(match[1].trim());
        i += 1;
      }
      blocks.push({ type: 'ordered_list', items });
      continue;
    }

    const paragraphLines: string[] = [];
    while (i < lines.length) {
      const line = lines[i].trim();
      if (!line) break;
      if (/^[-*+]\s+/.test(line) || /^\d+\.\s+/.test(line)) break;
      paragraphLines.push(line);
      i += 1;
    }
    blocks.push({ type: 'paragraph', text: paragraphLines.join(' ') });
  }

  return blocks;
}

function renderInlineMarkdown(text: string): React.ReactNode[] {
  const pattern = /(\[[^\]]+\]\(([^)]+)\)|`[^`]+`|\*\*[^*]+\*\*|\*[^*]+\*|https?:\/\/[^\s<]+)/g;
  const nodes: React.ReactNode[] = [];
  let cursor = 0;
  let match: RegExpExecArray | null;

  while ((match = pattern.exec(text)) !== null) {
    const full = match[0];
    const start = match.index;
    if (start > cursor) {
      nodes.push(text.slice(cursor, start));
    }

    if (full.startsWith('[') && match[2]) {
      const labelMatch = full.match(/^\[([^\]]+)\]/);
      const label = labelMatch?.[1] || full;
      const href = match[2];
      if (/^https?:\/\//i.test(href)) {
        nodes.push(
          <a key={`${start}-link`} href={href} target="_blank" rel="noopener noreferrer">
            {label}
          </a>
        );
      } else {
        nodes.push(label);
      }
    } else if (full.startsWith('`') && full.endsWith('`')) {
      nodes.push(<code key={`${start}-code`}>{full.slice(1, -1)}</code>);
    } else if (full.startsWith('**') && full.endsWith('**')) {
      nodes.push(<strong key={`${start}-bold`}>{full.slice(2, -2)}</strong>);
    } else if (full.startsWith('*') && full.endsWith('*')) {
      nodes.push(<em key={`${start}-italic`}>{full.slice(1, -1)}</em>);
    } else if (/^https?:\/\//i.test(full)) {
      nodes.push(
        <a key={`${start}-url`} href={full} target="_blank" rel="noopener noreferrer">
          {full}
        </a>
      );
    } else {
      nodes.push(full);
    }
    cursor = start + full.length;
  }

  if (cursor < text.length) {
    nodes.push(text.slice(cursor));
  }
  return nodes;
}
