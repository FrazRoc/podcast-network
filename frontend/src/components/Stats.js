import ShowOverlapMatrix from './ShowOverlapMatrix';
import GuestAppearanceChart from './GuestAppearanceChart';

export default function Stats() {
  return (
    <div className="min-h-screen bg-gray-100 font-sans">
      <header className="bg-white border-b border-gray-200 px-4 py-3 sm:px-6 sm:py-4 flex items-center gap-4">
        <a href="/" className="text-gray-400 hover:text-gray-600 text-sm">← Network</a>
        <h1 className="text-lg font-semibold text-gray-900">Stats</h1>
      </header>

      <div className="max-w-5xl mx-auto p-4 sm:p-6 space-y-6">

        <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
          <h2 className="text-base font-semibold text-gray-900 mb-1">Show Overlap</h2>
          <p className="text-sm text-gray-500 mb-4">
            How many guests the top 25 most-connected shows share with each other.
          </p>
          <ShowOverlapMatrix />
        </div>

        <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
          <h2 className="text-base font-semibold text-gray-900 mb-1">Guest Appearances</h2>
          <p className="text-sm text-gray-500 mb-4">
            How concentrated the network is — most people appear once, a few appear dozens of times.
          </p>
          <GuestAppearanceChart />
        </div>

      </div>
    </div>
  );
}
