import React from 'react';
import Link from '@docusaurus/Link';
import clsx from 'clsx';

type ButtonProps = {
  link: string;
  label: string;
  variant?: 'primary' | 'secondary';
  outline?: boolean;
  size?: 'default' | 'large';
  className?: string;
};

export default function Button({
  link,
  label,
  variant = 'primary',
  outline = false,
  size = 'default',
  className,
}: ButtonProps): React.ReactElement {
  const isPrimary = variant === 'primary';
  const isOutline = outline;
  const isLarge = size === 'large';
  return (
    <Link
      to={link}
      className={clsx(
        'landing-hero__link',
        isPrimary && !isOutline && 'landing-hero__link--primary',
        (variant === 'secondary' || isOutline) && 'landing-hero__link--secondary',
        isLarge && 'landing-cta__button',
        className
      )}
    >
      {label}
    </Link>
  );
}
