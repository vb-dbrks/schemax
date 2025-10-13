/**
 * Provider System Exports
 * 
 * Central export point for all providers and the registry.
 * Automatically registers all available providers.
 */

// Export base provider system
export * from './base';
export * from './registry';

// Export Unity provider
export * from './unity';

// Import providers for registration
import { ProviderRegistry } from './registry';
import { unityProvider } from './unity';

/**
 * Initialize and register all providers
 * This is called automatically when the module is imported
 */
export function initializeProviders(): void {
  // Register Unity Catalog provider
  ProviderRegistry.register(unityProvider);
  
  // Future providers will be registered here:
  // ProviderRegistry.register(hiveProvider);
  // ProviderRegistry.register(postgresProvider);
}

// Auto-initialize on import
initializeProviders();

