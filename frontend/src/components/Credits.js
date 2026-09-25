// Credits the free tiers ask for: logo.dev (company logos) and Wikimedia
// Commons (freely licensed photos and logos found through Wikidata).
export default function Credits({ className = '' }) {
  const link = 'underline hover:text-gray-600';
  return (
    <p className={`text-[11px] text-gray-400 ${className}`}>
      <a href="https://logo.dev" target="_blank" rel="noopener noreferrer" className={link}>Logos provided by Logo.dev</a>
      {' · '}
      Some photos and logos from{' '}
      <a href="https://commons.wikimedia.org" target="_blank" rel="noopener noreferrer" className={link}>Wikimedia Commons</a>
    </p>
  );
}
