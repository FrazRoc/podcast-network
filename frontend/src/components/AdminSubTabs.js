import React from 'react';

// Second-level tabs under an admin section (People → People / Duplicates,
// Companies → Companies / Merge suggestions). A tab is a link when it is its
// own page (href) or a button when it switches view within one (onClick).
export default function AdminSubTabs({ tabs, active }) {
  const cls = (id) => `px-3 py-1.5 rounded-lg text-sm ${active === id
    ? 'bg-gray-900 text-white'
    : 'text-gray-600 hover:bg-gray-200'}`;
  return (
    <div className="flex gap-2 mb-4">
      {tabs.map(t => t.href
        ? <a key={t.id} href={t.href} className={cls(t.id)}>{t.label}</a>
        : <button key={t.id} onClick={t.onClick} className={cls(t.id)}>{t.label}</button>)}
    </div>
  );
}
