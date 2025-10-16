/**
 * Provider Registry
 * 
 * Central registry for all available catalog providers.
 * Providers must register themselves here to be available in the system.
 */

import { Provider } from './base/provider';

/**
 * Registry for managing catalog providers
 */
class ProviderRegistryClass {
  private providers: Map<string, Provider> = new Map();
  
  /**
   * Register a provider
   * @param provider - Provider to register
   * @throws Error if provider with same ID is already registered
   */
  register(provider: Provider): void {
    if (this.providers.has(provider.info.id)) {
      throw new Error(
        `Provider with ID '${provider.info.id}' is already registered`
      );
    }
    
    this.providers.set(provider.info.id, provider);
    console.log(`[Schematic] Registered provider: ${provider.info.name} (${provider.info.id})`);
  }
  
  /**
   * Get a provider by ID
   * @param providerId - Provider ID (e.g., 'unity', 'hive', 'postgres')
   * @returns Provider instance or undefined
   */
  get(providerId: string): Provider | undefined {
    return this.providers.get(providerId);
  }
  
  /**
   * Get all registered providers
   * @returns Array of all providers
   */
  getAll(): Provider[] {
    return Array.from(this.providers.values());
  }
  
  /**
   * Get all provider IDs
   * @returns Array of provider IDs
   */
  getAllIds(): string[] {
    return Array.from(this.providers.keys());
  }
  
  /**
   * Check if a provider is registered
   * @param providerId - Provider ID to check
   * @returns True if provider is registered
   */
  has(providerId: string): boolean {
    return this.providers.has(providerId);
  }
  
  /**
   * Check if a provider supports a specific operation
   * @param providerId - Provider ID
   * @param operation - Operation type (e.g., 'unity.add_catalog')
   * @returns True if provider supports the operation
   */
  supports(providerId: string, operation: string): boolean {
    const provider = this.get(providerId);
    if (!provider) {
      return false;
    }
    
    return provider.capabilities.supportedOperations.includes(operation);
  }
  
  /**
   * Get provider that supports a specific operation
   * @param operation - Operation type with provider prefix
   * @returns Provider or undefined
   */
  getProviderForOperation(operation: string): Provider | undefined {
    // Extract provider ID from operation (e.g., 'unity.add_catalog' -> 'unity')
    const providerId = operation.split('.')[0];
    return this.get(providerId);
  }
  
  /**
   * Clear all registered providers (useful for testing)
   */
  clear(): void {
    this.providers.clear();
  }
  
  /**
   * Unregister a provider
   * @param providerId - Provider ID to unregister
   */
  unregister(providerId: string): void {
    this.providers.delete(providerId);
  }
}

// Singleton instance
export const ProviderRegistry = new ProviderRegistryClass();

