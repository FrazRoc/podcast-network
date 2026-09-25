import ShowOverlapMatrix from './ShowOverlapMatrix';
import GuestAppearanceChart from './GuestAppearanceChart';
import GuestReachChart from './GuestReachChart';
import BridgeChart from './BridgeChart';
import GuestMomentumGrid from './GuestMomentumGrid';
import GuestCohortChart from './GuestCohortChart';
import ShowTimelineGantt from './ShowTimelineGantt';
import EpisodeCalendarHeatmap from './EpisodeCalendarHeatmap';
import ShowGuestMixChart from './ShowGuestMixChart';
import RevolvingDoorChart from './RevolvingDoorChart';
import TopOrganizationsChart from './TopOrganizationsChart';
import GuestMixByYearChart from './GuestMixByYearChart';
import GuestRolesChart from './GuestRolesChart';

export default function Stats() {
  return (
    <div className="h-screen overflow-y-auto bg-gray-100 font-sans">
      <header className="bg-white border-b border-gray-200 px-4 py-3 sm:px-6 sm:py-4 flex items-center gap-4">
        <a href="/" className="text-gray-400 hover:text-gray-600 text-sm">← Network</a>
        <h1 className="text-lg font-semibold text-gray-900">Stats</h1>
      </header>

      <div className="max-w-5xl mx-auto p-4 sm:p-6 space-y-6">

        {/* From guests' roles and organisations (backend/org_stats.py) */}
        <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wide pt-2">Who the guests are</h2>

        <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
          <h2 className="text-base font-semibold text-gray-900 mb-1">Who Each Show Books</h2>
          <p className="text-sm text-gray-500 mb-4">
            Each show's guests by the kind of organisation they work for. Some shows are industry
            shows, some are policy or academic ones; this is where you can see which.
          </p>
          <ShowGuestMixChart />
        </div>

        <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
          <h2 className="text-base font-semibold text-gray-900 mb-1">The Revolving Door</h2>
          <p className="text-sm text-gray-500 mb-4">
            Where guests used to work and where they work now, by kind of organisation.
          </p>
          <RevolvingDoorChart />
        </div>

        <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
          <h2 className="text-base font-semibold text-gray-900 mb-1">Most-Booked Organisations</h2>
          <p className="text-sm text-gray-500 mb-4">
            The organisations whose people turn up most, by how many of them have been guests or how
            many different shows they've been on.
          </p>
          <TopOrganizationsChart />
        </div>

        <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
          <h2 className="text-base font-semibold text-gray-900 mb-1">Guest Mix by Year</h2>
          <p className="text-sm text-gray-500 mb-4">
            The share of guest appearances from each kind of organisation, year by year. Most of it
            barely moves; government guests are the exception.
          </p>
          <GuestMixByYearChart />
        </div>

        <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
          <h2 className="text-base font-semibold text-gray-900 mb-1">What Guests Do</h2>
          <p className="text-sm text-gray-500 mb-4">
            Guests by the kind of role they hold: founders, executives, investors, academics,
            journalists and officials.
          </p>
          <GuestRolesChart />
        </div>

        <h2 className="text-xs font-semibold text-gray-400 uppercase tracking-wide pt-4">Shows and appearances</h2>

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
