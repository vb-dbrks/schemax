/**
 * Tests for RichComment component - Rich text rendering with collapse/expand
 */

import React from 'react';
import { render, screen } from '@testing-library/react';
import { describe, test, expect, jest, beforeEach } from '@jest/globals';
import userEvent from '@testing-library/user-event';
import { RichComment } from '../../../src/webview/components/RichComment';

describe('RichComment', () => {
  // ─── Returns null for empty / whitespace text ───────────────────────

  describe('empty text handling', () => {
    test('returns null for empty string', () => {
      const { container } = render(<RichComment text="" />);
      expect(container.innerHTML).toBe('');
    });

    test('returns null for whitespace-only string', () => {
      const { container } = render(<RichComment text="   " />);
      expect(container.innerHTML).toBe('');
    });

    test('returns null for newlines-only string', () => {
      const { container } = render(<RichComment text={"\n\n\n"} />);
      expect(container.innerHTML).toBe('');
    });

    test('returns null for undefined-like falsy text', () => {
      const { container } = render(<RichComment text={undefined as unknown as string} />);
      expect(container.innerHTML).toBe('');
    });
  });

  // ─── Simple paragraph rendering ─────────────────────────────────────

  describe('paragraph rendering', () => {
    test('renders a simple paragraph', () => {
      render(<RichComment text="Hello world" />);
      expect(screen.getByText('Hello world')).toBeInTheDocument();
    });

    test('wraps paragraph text in a <p> with correct class', () => {
      const { container } = render(<RichComment text="Some text" />);
      const paragraph = container.querySelector('p.rich-comment__paragraph');
      expect(paragraph).not.toBeNull();
      expect(paragraph!.textContent).toBe('Some text');
    });

    test('joins multiline paragraph text with spaces', () => {
      render(<RichComment text={"Line one\nLine two"} />);
      expect(screen.getByText('Line one Line two')).toBeInTheDocument();
    });

    test('renders root div with rich-comment class', () => {
      const { container } = render(<RichComment text="Test" />);
      expect(container.querySelector('.rich-comment')).not.toBeNull();
    });
  });

  // ─── Unordered list rendering ───────────────────────────────────────

  describe('unordered list rendering', () => {
    test('renders dash-prefixed lines as an unordered list', () => {
      const text = '- Item one\n- Item two\n- Item three';
      const { container } = render(<RichComment text={text} />);
      const ul = container.querySelector('ul.rich-comment__list');
      expect(ul).not.toBeNull();
      const items = ul!.querySelectorAll('li');
      expect(items).toHaveLength(3);
      expect(items[0].textContent).toBe('Item one');
      expect(items[1].textContent).toBe('Item two');
      expect(items[2].textContent).toBe('Item three');
    });

    test('renders asterisk-prefixed lines as an unordered list', () => {
      const text = '* Alpha\n* Beta';
      const { container } = render(<RichComment text={text} />);
      const ul = container.querySelector('ul.rich-comment__list');
      expect(ul).not.toBeNull();
      const items = ul!.querySelectorAll('li');
      expect(items).toHaveLength(2);
      expect(items[0].textContent).toBe('Alpha');
      expect(items[1].textContent).toBe('Beta');
    });

    test('renders plus-prefixed lines as an unordered list', () => {
      const text = '+ First\n+ Second';
      const { container } = render(<RichComment text={text} />);
      const ul = container.querySelector('ul.rich-comment__list');
      expect(ul).not.toBeNull();
      expect(ul!.querySelectorAll('li')).toHaveLength(2);
    });
  });

  // ─── Ordered list rendering ─────────────────────────────────────────

  describe('ordered list rendering', () => {
    test('renders numbered lines as an ordered list', () => {
      const text = '1. First\n2. Second\n3. Third';
      const { container } = render(<RichComment text={text} />);
      const ol = container.querySelector('ol.rich-comment__list--ordered');
      expect(ol).not.toBeNull();
      const items = ol!.querySelectorAll('li');
      expect(items).toHaveLength(3);
      expect(items[0].textContent).toBe('First');
      expect(items[1].textContent).toBe('Second');
      expect(items[2].textContent).toBe('Third');
    });

    test('handles multi-digit numbers', () => {
      const text = '10. Tenth\n11. Eleventh';
      const { container } = render(<RichComment text={text} />);
      const ol = container.querySelector('ol');
      expect(ol).not.toBeNull();
      expect(ol!.querySelectorAll('li')).toHaveLength(2);
    });
  });

  // ─── Multiple block types ──────────────────────────────────────────

  describe('multiple blocks', () => {
    test('renders a paragraph followed by an unordered list', () => {
      const text = 'Introduction text\n\n- Item A\n- Item B';
      const { container } = render(<RichComment text={text} />);
      const paragraph = container.querySelector('p.rich-comment__paragraph');
      const ul = container.querySelector('ul.rich-comment__list');
      expect(paragraph).not.toBeNull();
      expect(paragraph!.textContent).toBe('Introduction text');
      expect(ul).not.toBeNull();
      expect(ul!.querySelectorAll('li')).toHaveLength(2);
    });

    test('renders a paragraph followed by an ordered list', () => {
      const text = 'Steps:\n\n1. Do this\n2. Do that';
      const { container } = render(<RichComment text={text} />);
      const paragraph = container.querySelector('p');
      const ol = container.querySelector('ol');
      expect(paragraph).not.toBeNull();
      expect(ol).not.toBeNull();
      expect(ol!.querySelectorAll('li')).toHaveLength(2);
    });

    test('renders multiple paragraphs separated by blank lines', () => {
      const text = 'First paragraph\n\nSecond paragraph\n\nThird paragraph';
      const { container } = render(<RichComment text={text} />);
      const paragraphs = container.querySelectorAll('p.rich-comment__paragraph');
      expect(paragraphs).toHaveLength(3);
    });

    test('renders unordered list followed by paragraph', () => {
      const text = '- Item one\n- Item two\n\nConclusion here';
      const { container } = render(<RichComment text={text} />);
      expect(container.querySelector('ul')).not.toBeNull();
      expect(container.querySelector('p')).not.toBeNull();
    });

    test('renders mixed: paragraph, unordered list, ordered list', () => {
      const text = 'Intro\n\n- Bullet A\n- Bullet B\n\n1. Step one\n2. Step two';
      const { container } = render(<RichComment text={text} />);
      expect(container.querySelectorAll('p')).toHaveLength(1);
      expect(container.querySelector('ul')).not.toBeNull();
      expect(container.querySelector('ol')).not.toBeNull();
    });
  });

  // ─── Collapse behavior ─────────────────────────────────────────────

  describe('collapse behavior', () => {
    test('does not show toggle button for short text', () => {
      render(<RichComment text="Short text" />);
      expect(screen.queryByRole('button')).toBeNull();
    });

    test('does not add is-collapsed class for short text', () => {
      const { container } = render(<RichComment text="Short text" />);
      const content = container.querySelector('.rich-comment__content');
      expect(content!.classList.contains('is-collapsed')).toBe(false);
    });

    test('shows toggle button when text exceeds collapseThreshold', () => {
      const longText = 'a'.repeat(400);
      render(<RichComment text={longText} />);
      expect(screen.getByRole('button', { name: 'Show more' })).toBeInTheDocument();
    });

    test('shows toggle button when line count exceeds collapsedLines + 1', () => {
      // Default collapsedLines = 7, so > 8 lines triggers collapse
      const lines = Array.from({ length: 10 }, (_, i) => `Line ${i + 1}`).join('\n');
      render(<RichComment text={lines} />);
      expect(screen.getByRole('button', { name: 'Show more' })).toBeInTheDocument();
    });

    test('adds is-collapsed class when collapsed', () => {
      const longText = 'a'.repeat(400);
      const { container } = render(<RichComment text={longText} />);
      const content = container.querySelector('.rich-comment__content');
      expect(content!.classList.contains('is-collapsed')).toBe(true);
    });

    test('sets --collapsed-lines CSS variable', () => {
      const { container } = render(<RichComment text="Hello" collapsedLines={5} />);
      const content = container.querySelector('.rich-comment__content') as HTMLElement;
      expect(content.style.getPropertyValue('--collapsed-lines')).toBe('5');
    });

    test('respects custom collapseThreshold', () => {
      const text = 'a'.repeat(50);
      render(<RichComment text={text} collapseThreshold={30} />);
      expect(screen.getByRole('button', { name: 'Show more' })).toBeInTheDocument();
    });

    test('respects custom collapsedLines', () => {
      const lines = Array.from({ length: 5 }, (_, i) => `Line ${i}`).join('\n');
      render(<RichComment text={lines} collapsedLines={3} />);
      expect(screen.getByRole('button', { name: 'Show more' })).toBeInTheDocument();
    });

    test('does not collapse when exactly at threshold boundary', () => {
      // collapseThreshold=360 means > 360 triggers collapse
      const text = 'a'.repeat(360);
      render(<RichComment text={text} collapseThreshold={360} />);
      // Exactly 360 chars should NOT trigger collapse (condition is >)
      // But line count is 1 which is <= collapsedLines + 1 (8)
      expect(screen.queryByRole('button')).toBeNull();
    });
  });

  // ─── Show more / show less toggle ──────────────────────────────────

  describe('show more / show less toggle', () => {
    test('clicking "Show more" expands and changes to "Show less"', async () => {
      const user = userEvent.setup();
      const longText = 'a'.repeat(400);
      render(<RichComment text={longText} />);

      const button = screen.getByRole('button', { name: 'Show more' });
      await user.click(button);

      expect(screen.getByRole('button', { name: 'Show less' })).toBeInTheDocument();
    });

    test('clicking "Show less" collapses and changes back to "Show more"', async () => {
      const user = userEvent.setup();
      const longText = 'a'.repeat(400);
      render(<RichComment text={longText} />);

      const button = screen.getByRole('button', { name: 'Show more' });
      await user.click(button);
      await user.click(screen.getByRole('button', { name: 'Show less' }));

      expect(screen.getByRole('button', { name: 'Show more' })).toBeInTheDocument();
    });

    test('removes is-collapsed class when expanded', async () => {
      const user = userEvent.setup();
      const longText = 'a'.repeat(400);
      const { container } = render(<RichComment text={longText} />);

      await user.click(screen.getByRole('button', { name: 'Show more' }));

      const content = container.querySelector('.rich-comment__content');
      expect(content!.classList.contains('is-collapsed')).toBe(false);
    });

    test('re-adds is-collapsed class when collapsed again', async () => {
      const user = userEvent.setup();
      const longText = 'a'.repeat(400);
      const { container } = render(<RichComment text={longText} />);

      await user.click(screen.getByRole('button', { name: 'Show more' }));
      await user.click(screen.getByRole('button', { name: 'Show less' }));

      const content = container.querySelector('.rich-comment__content');
      expect(content!.classList.contains('is-collapsed')).toBe(true);
    });

    test('resets expanded state when text changes', async () => {
      const user = userEvent.setup();
      const longText = 'a'.repeat(400);
      const { rerender } = render(<RichComment text={longText} />);

      await user.click(screen.getByRole('button', { name: 'Show more' }));
      expect(screen.getByRole('button', { name: 'Show less' })).toBeInTheDocument();

      const newLongText = 'b'.repeat(400);
      rerender(<RichComment text={newLongText} />);

      // After text change, should reset to collapsed state
      expect(screen.getByRole('button', { name: 'Show more' })).toBeInTheDocument();
    });
  });

  // ─── Bold text rendering (**text**) ────────────────────────────────

  describe('bold text rendering', () => {
    test('renders **bold** as <strong>', () => {
      render(<RichComment text="This is **bold** text" />);
      const strong = document.querySelector('strong');
      expect(strong).not.toBeNull();
      expect(strong!.textContent).toBe('bold');
    });

    test('renders surrounding text alongside bold', () => {
      render(<RichComment text="Before **bold** after" />);
      expect(screen.getByText(/Before/)).toBeInTheDocument();
      expect(screen.getByText(/after/)).toBeInTheDocument();
    });

    test('renders multiple bold segments', () => {
      render(<RichComment text="**first** and **second**" />);
      const strongs = document.querySelectorAll('strong');
      expect(strongs).toHaveLength(2);
      expect(strongs[0].textContent).toBe('first');
      expect(strongs[1].textContent).toBe('second');
    });
  });

  // ─── Italic text rendering (*text*) ────────────────────────────────

  describe('italic text rendering', () => {
    test('renders *italic* as <em>', () => {
      render(<RichComment text="This is *italic* text" />);
      const em = document.querySelector('em');
      expect(em).not.toBeNull();
      expect(em!.textContent).toBe('italic');
    });
  });

  // ─── Code inline rendering (`code`) ────────────────────────────────

  describe('inline code rendering', () => {
    test('renders `code` as <code>', () => {
      render(<RichComment text="Use `console.log` for debugging" />);
      const code = document.querySelector('code');
      expect(code).not.toBeNull();
      expect(code!.textContent).toBe('console.log');
    });

    test('renders surrounding text alongside code', () => {
      render(<RichComment text="Run `npm install` first" />);
      expect(screen.getByText(/Run/)).toBeInTheDocument();
      expect(screen.getByText(/first/)).toBeInTheDocument();
    });

    test('renders multiple inline code segments', () => {
      render(<RichComment text="`foo` and `bar`" />);
      const codes = document.querySelectorAll('code');
      expect(codes).toHaveLength(2);
      expect(codes[0].textContent).toBe('foo');
      expect(codes[1].textContent).toBe('bar');
    });
  });

  // ─── Link rendering ────────────────────────────────────────────────

  describe('link rendering', () => {
    test('renders bare URLs as clickable links', () => {
      render(<RichComment text="Visit https://example.com for more" />);
      const link = document.querySelector('a');
      expect(link).not.toBeNull();
      expect(link!.href).toBe('https://example.com/');
      expect(link!.target).toBe('_blank');
      expect(link!.rel).toContain('noopener');
    });

    test('renders markdown links with labels', () => {
      render(<RichComment text="Click [here](https://example.com) to visit" />);
      const link = document.querySelector('a');
      expect(link).not.toBeNull();
      expect(link!.textContent).toBe('here');
      expect(link!.href).toBe('https://example.com/');
    });

    test('renders markdown link with non-http href as plain text', () => {
      render(<RichComment text="See [label](not-a-url) for details" />);
      const link = document.querySelector('a');
      expect(link).toBeNull();
      expect(screen.getByText(/label/)).toBeInTheDocument();
    });

    test('renders http link in markdown format', () => {
      render(<RichComment text="Go to [site](http://example.com)" />);
      const link = document.querySelector('a');
      expect(link).not.toBeNull();
      expect(link!.href).toBe('http://example.com/');
    });
  });

  // ─── Mixed formatting ──────────────────────────────────────────────

  describe('mixed formatting', () => {
    test('renders bold and code in the same paragraph', () => {
      render(<RichComment text="Use **important** function `doStuff()`" />);
      expect(document.querySelector('strong')).not.toBeNull();
      expect(document.querySelector('code')).not.toBeNull();
    });

    test('renders inline formatting within list items', () => {
      const text = '- Use **bold** here\n- And `code` here';
      const { container } = render(<RichComment text={text} />);
      const items = container.querySelectorAll('li');
      expect(items[0].querySelector('strong')).not.toBeNull();
      expect(items[1].querySelector('code')).not.toBeNull();
    });

    test('renders inline formatting within ordered list items', () => {
      const text = '1. First **bold**\n2. Second `code`';
      const { container } = render(<RichComment text={text} />);
      const items = container.querySelectorAll('li');
      expect(items[0].querySelector('strong')).not.toBeNull();
      expect(items[1].querySelector('code')).not.toBeNull();
    });

    test('renders bold, italic, code, and links together', () => {
      const text = '**Bold** and *italic* with `code` and https://example.com';
      render(<RichComment text={text} />);
      expect(document.querySelector('strong')).not.toBeNull();
      expect(document.querySelector('em')).not.toBeNull();
      expect(document.querySelector('code')).not.toBeNull();
      expect(document.querySelector('a')).not.toBeNull();
    });
  });

  // ─── Edge cases ────────────────────────────────────────────────────

  describe('edge cases', () => {
    test('skips blank lines between blocks', () => {
      const text = 'Paragraph one\n\n\n\nParagraph two';
      const { container } = render(<RichComment text={text} />);
      const paragraphs = container.querySelectorAll('p');
      expect(paragraphs).toHaveLength(2);
    });

    test('handles text with only a list', () => {
      const text = '- Only\n- A\n- List';
      const { container } = render(<RichComment text={text} />);
      expect(container.querySelectorAll('p')).toHaveLength(0);
      expect(container.querySelector('ul')).not.toBeNull();
    });

    test('paragraph breaks on encountering a list marker', () => {
      const text = 'Paragraph text\n- List item';
      const { container } = render(<RichComment text={text} />);
      expect(container.querySelector('p')).not.toBeNull();
      expect(container.querySelector('ul')).not.toBeNull();
    });

    test('paragraph breaks on encountering an ordered list marker', () => {
      const text = 'Paragraph text\n1. First step';
      const { container } = render(<RichComment text={text} />);
      expect(container.querySelector('p')).not.toBeNull();
      expect(container.querySelector('ol')).not.toBeNull();
    });

    test('plain text with no formatting renders as-is', () => {
      render(<RichComment text="Just plain text with no special chars" />);
      expect(screen.getByText('Just plain text with no special chars')).toBeInTheDocument();
    });

    test('toggle button has correct class', () => {
      const longText = 'a'.repeat(400);
      const { container } = render(<RichComment text={longText} />);
      const button = container.querySelector('.rich-comment__toggle');
      expect(button).not.toBeNull();
    });

    test('toggle button has type="button"', () => {
      const longText = 'a'.repeat(400);
      render(<RichComment text={longText} />);
      const button = screen.getByRole('button');
      expect(button.getAttribute('type')).toBe('button');
    });
  });
});
