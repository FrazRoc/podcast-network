// A plain DATE like "2026-09-10" has no time component, so new Date(...)
// parses it as UTC midnight — toLocaleDateString() then shows the wrong
// day for anyone west of UTC. Parse the parts directly as local time instead.
export const formatDateOnly = (dateStr) => {
  if (!dateStr) return null;
  const [year, month, day] = dateStr.split('-').map(Number);
  return new Date(year, month - 1, day).toLocaleDateString();
};
