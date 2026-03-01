// Telemetry stub for future implementation
export function trackEvent(eventName: string, properties?: Record<string, unknown>): void {
  // Future: Send telemetry
  console.warn(`[Telemetry] ${eventName}`, properties);
}
