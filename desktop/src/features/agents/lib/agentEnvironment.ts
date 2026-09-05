/** Match the backend's last case-insensitive key in BTreeMap order. */
export function readAgentEnvCaseInsensitive(
  environment: Record<string, string>,
  key: string,
): string | undefined {
  const folded = key.toLowerCase();
  const match = Object.keys(environment)
    .sort()
    .reverse()
    .find((candidate) => candidate.toLowerCase() === folded);
  return match === undefined ? undefined : environment[match];
}
