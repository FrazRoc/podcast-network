const NAV_LINKS = [
  { href: '/admin', label: 'Suggestions' },
  { href: '/admin/images', label: 'Images' },
  { href: '/admin/shows', label: 'Shows' },
  { href: '/admin/episodes', label: 'Episodes' },
  { href: '/admin/people', label: 'People' },
];

export default function AdminHeader({ active, right }) {
  return (
    <header className="bg-white border-b border-gray-200 px-6 py-4 flex items-center justify-between">
      <div className="flex items-center gap-4">
        <a href="/" className="text-gray-400 hover:text-gray-600 text-sm">← Network</a>
        {NAV_LINKS.map(link => (
          <a
            key={link.href}
            href={link.href}
            className={active === link.label
              ? 'text-gray-900 font-semibold text-sm'
              : 'text-gray-400 hover:text-gray-600 text-sm'}
          >
            {link.label}
          </a>
        ))}
      </div>
      {right}
    </header>
  );
}
