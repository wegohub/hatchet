import { getErrorStatus, shouldRetryQueryError } from '@/lib/error-utils';

export function getRunQueryRetryOptions(
  wasRedirectedFromTrigger: boolean,
  hasLoadedRun: () => boolean,
) {
  const isAwaitingReplication = (error: unknown) =>
    wasRedirectedFromTrigger &&
    !hasLoadedRun() &&
    getErrorStatus(error) === 404;

  return {
    retry: (failureCount: number, error: unknown) => {
      // A trigger can return its ID before the run reaches the analytics store.
      if (isAwaitingReplication(error)) {
        return failureCount < 10;
      }

      return shouldRetryQueryError(error);
    },
    retryDelay: (attempt: number, error: unknown) =>
      isAwaitingReplication(error) ? 200 : Math.min(1000 * 2 ** attempt, 30000),
  };
}
