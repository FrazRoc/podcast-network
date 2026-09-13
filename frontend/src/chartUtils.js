// Sequential single-hue scale (teal) — light to dark by magnitude.
// Shared by any chart encoding magnitude via color (never identity/category).
export const tealScale = (t) => {
  const lightness = 92 - t * 62; // 92% (near-white) -> 30% (deep teal)
  return `hsl(173, 65%, ${lightness}%)`;
};
