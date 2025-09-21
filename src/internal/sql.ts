const IDENTIFIER_PATTERN = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

export function assertValidIdentifier(value: string): void {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new Error(`Invalid SQL identifier: ${value}`);
  }
}

export function quoteIdentifier(value: string): string {
  assertValidIdentifier(value);
  return `"${value}"`;
}
