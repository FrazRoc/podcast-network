// Sequential single-hue scale (teal) — light to dark by magnitude.
// Shared by any chart encoding magnitude via color (never identity/category).
export const tealScale = (t) => {
  const lightness = 92 - t * 62; // 92% (near-white) -> 30% (deep teal)
  return `hsl(173, 65%, ${lightness}%)`;
};

// Categorical colours for organisation types and role kinds (identity, not
// magnitude). Muted, distinguishable set; grey is reserved for "other".
export const ORG_TYPE_COLORS = {
  company: '#4e79a7', investor: '#59a14f', nonprofit: '#edc948', research: '#76b7b2',
  academic: '#b07aa1', government: '#e15759', media: '#f28e2b', association: '#9c755f',
};
export const ORG_TYPE_LABELS = {
  company: 'Company', investor: 'Investor', nonprofit: 'Nonprofit', research: 'Research / think tank',
  academic: 'Academic', government: 'Government', media: 'Media', association: 'Association',
};
export const ROLE_COLORS = {
  founder: '#f28e2b', ceo: '#e15759', executive: '#4e79a7', investor: '#59a14f',
  advisor: '#edc948', analyst: '#76b7b2', academic: '#b07aa1', journalist: '#ff9da7',
  activist: '#8cd17d', official: '#9c755f', engineer: '#499894', other: '#bab0ac',
};
