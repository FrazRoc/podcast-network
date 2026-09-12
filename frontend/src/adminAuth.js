const STORAGE_KEY = 'admin_password';

export const getAdminPassword = () => localStorage.getItem(STORAGE_KEY) || '';
export const setAdminPassword = (pw) => localStorage.setItem(STORAGE_KEY, pw);
export const clearAdminPassword = () => localStorage.removeItem(STORAGE_KEY);

// Wraps fetch for admin API calls: attaches the stored password header, and
// forces a re-login if the server rejects it (wrong/expired password).
export async function adminFetch(url, options = {}) {
  const headers = { ...(options.headers || {}), 'X-Admin-Password': getAdminPassword() };
  const res = await fetch(url, { ...options, headers });
  if (res.status === 401) {
    clearAdminPassword();
    window.location.reload();
  }
  return res;
}
