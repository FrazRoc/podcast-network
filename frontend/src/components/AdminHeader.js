const NAV_LINKS = [
  { href: '/admin', label: 'Suggestions' },
  { href: '/admin/images', label: 'Images' },
  { href: '/admin/shows', label: 'Shows' },
  { href: '/admin/episodes', label: 'Episodes' },
  { href: '/admin/people', label: 'People' },
];

export default function AdminHeader({ active, right }) {
  return (
    <header className="bg-white border-b border-gray-200 px-4 py-3 sm:px-6 sm:py-4 flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
      <div className="flex items-center gap-3 sm:gap-4 overflow-x-auto whitespace-nowrap -mx-4 px-4 sm:mx-0 sm:px-0">
        <a href="/" className="text-gray-400 hover:text-gray-600 text-sm flex-shrink-0">← Network</a>
        {NAV_LINKS.map(link => (
          <a
            key={link.href}
            href={link.href}
            className={`flex-shrink-0 text-sm ${active === link.label
              ? 'text-gray-900 font-semibold'
              : 'text-gray-400 hover:text-gray-600'}`}
          >
            {link.label}
          </a>
        ))}
      </div>
      {right}
    </header>
  );
}
