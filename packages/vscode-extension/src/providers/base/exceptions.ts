/**
 * Custom exceptions for provider operations
 */

export class CircularDependencyError extends Error {
  public cycles: string[];

  constructor(cycles: string[]) {
    const message = 'Circular dependencies detected:\n' + cycles.join('\n');
    super(message);
    this.name = 'CircularDependencyError';
    this.cycles = cycles;
  }
}

