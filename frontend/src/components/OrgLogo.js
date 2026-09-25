import { useState } from 'react';
import { API_BASE_URL } from '../config';

// An organisation's logo (backend /api/logo/{org_id}: logo.dev by its
// website, else its Wikimedia Commons logo, else its parent's), or its
// initial when there's none. `size` is in pixels.
export default function OrgLogo({ orgId, name, size = 20, className = '' }) {
  const [failed, setFailed] = useState(false);
  const box = { width: size, height: size };
  if (!orgId || failed) {
    return (
      <span style={{ ...box, fontSize: Math.max(9, Math.round(size * 0.5)) }}
        className={`inline-flex items-center justify-center flex-shrink-0 rounded bg-gray-100 text-gray-400 font-semibold ${className}`}
        aria-hidden="true">
        {(name || '?').replace(/^the\s+/i, '').charAt(0).toUpperCase()}
      </span>
    );
  }
  return (
    <img src={`${API_BASE_URL}/api/logo/${orgId}`} alt="" loading="lazy" style={box}
      onError={() => setFailed(true)}
      className={`flex-shrink-0 rounded bg-white object-contain ${className}`} />
  );
}
