// Telemetry stub for future implementation
export function trackEvent(eventName: string, properties?: Record<string, any>): void {
  // Future: Send telemetry
  console.log(`[Telemetry] ${eventName}`, properties);
}

