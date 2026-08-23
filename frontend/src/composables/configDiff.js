export const cloneConfigSnapshot = (value) => JSON.parse(JSON.stringify(value || {}));

export const configUpdatesSince = (current, baseline) => {
  const source = current || {};
  if (!baseline) return cloneConfigSnapshot(source);

  return Object.fromEntries(
    Object.entries(source).filter(([key, value]) => JSON.stringify(value) !== JSON.stringify(baseline[key]))
  );
};
