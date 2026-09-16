import ShowOverlapMatrix from './ShowOverlapMatrix';
import GuestAppearanceChart from './GuestAppearanceChart';
import GuestReachChart from './GuestReachChart';
import BridgeChart from './BridgeChart';
import GuestMomentumGrid from './GuestMomentumGrid';
import GuestCohortChart from './GuestCohortChart';
import ShowTimelineGantt from './ShowTimelineGantt';
import EpisodeCalendarHeatmap from './EpisodeCalendarHeatmap';

export default function Stats() {
  return (
    <div className="h-screen overflow-y-auto bg-gray-100 font-sans">
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
          <h2 className="text-base font-semibold text-gray-900 mb-1">Who's On Right Now</h2>
          <p className="text-sm text-gray-500 mb-4">
            The sixteen guests with the most appearances in the last 18 months,
            by year. Guests who have appeared on at least four different shows.
          </p>
          <GuestMomentumGrid />
        </div>

        <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
          <h2 className="text-base font-semibold text-gray-900 mb-1">Where Each Year's Guests Came From</h2>
          <p className="text-sm text-gray-500 mb-4">
            Every year's guest roster, split by the year each of those guests
            first appeared on any show in the network.
          </p>
          <GuestCohortChart />
        </div>

        <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
          <h2 className="text-base font-semibold text-gray-900 mb-1">Network Reach</h2>
          <p className="text-sm text-gray-500 mb-4">
            How many shows each person is one step from: the shows they appear on,
            plus every show their fellow guests appear on. Ordered by how many
            shows they appear on themselves.
          </p>
          <BridgeChart />
        </div>

        <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
          <h2 className="text-base font-semibold text-gray-900 mb-1">Guest Reach</h2>
          <p className="text-sm text-gray-500 mb-4">
            Whether a guest turns up once on many shows or returns to the same few.
            Above the dashed line means they came back.
          </p>
          <GuestReachChart />
        </div>

        <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
          <h2 className="text-base font-semibold text-gray-900 mb-1">Guest Appearances</h2>
          <p className="text-sm text-gray-500 mb-4">
            How concentrated the network is — most people appear once, a few appear dozens of times.
          </p>
          <GuestAppearanceChart />
        </div>

        <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
          <h2 className="text-base font-semibold text-gray-900 mb-1">Show Timelines</h2>
          <p className="text-sm text-gray-500 mb-4">
            When each show started publishing, how often, and whether it's still active.
          </p>
          <ShowTimelineGantt />
        </div>

        <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
          <h2 className="text-base font-semibold text-gray-900 mb-1">Episode Activity</h2>
          <p className="text-sm text-gray-500 mb-4">
            Episodes published per day across the whole network.
          </p>
          <EpisodeCalendarHeatmap />
        </div>

      </div>
    </div>
  );
}
