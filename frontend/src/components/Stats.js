import { useState, useEffect } from 'react';
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
import { TopicCategoriesByYearChart, TopicAreasByYearChart, RisingTopicsChart, ShowTopicMixChart, MostDiscussedChart } from './TopicStatsCharts';
import { SiteShell } from './SiteHeader';

function Section({ title, children, description }) {
  return (
    <div className="bg-white rounded-2xl border border-gray-200 p-4 sm:p-6">
      <h2 className="text-base font-semibold text-gray-900 mb-1">{title}</h2>
      <p className="text-sm text-gray-500 mb-4">{description}</p>
      {children}
    </div>
  );
}

// People, Shows and Topics as tabs, like the Network page (?view=shows or
// ?view=topics in the address bar). Only the open tab's charts mount, so
// only they load.
const VIEWS = ['people', 'shows', 'topics'];
function initialView() {
  try {
    const v = new URLSearchParams(window.location.search).get('view');
    return VIEWS.includes(v) ? v : 'people';
  } catch {
    return 'people';   // URLSearchParams is absent in some embedded webviews
  }
}

export default function Stats() {
  const [view, setView] = useState(initialView);

  // replaceState, as on the Network page: switching tabs shouldn't stack up
  // back-button presses.
  useEffect(() => {
    try {
      const url = new URL(window.location.href);
      if (view !== 'people') url.searchParams.set('view', view);
      else url.searchParams.delete('view');
      window.history.replaceState(null, '', url.toString());
    } catch {
      /* history is unavailable in some sandboxed frames; the tab still works */
    }
  }, [view]);

  return (
    <SiteShell width="max-w-5xl">
      <h1 className="text-2xl font-bold text-gray-900">Stats</h1>
      <div className="flex rounded-lg bg-gray-200/70 p-0.5 text-sm max-w-sm">
        {[['people', 'People'], ['shows', 'Shows'], ['topics', 'Topics']].map(([mode, label]) => (
          <button key={mode} onClick={() => setView(mode)}
            className={`flex-1 rounded-md py-1.5 font-medium transition-colors ${
              view === mode ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}>
            {label}
          </button>
        ))}
      </div>

      {view === 'people' ? (
        <>
          <Section title="Most-Booked Organisations"
            description="The organisations whose people turn up most, by how many of them have been guests or how many different shows they've been on.">
            <TopOrganizationsChart />
          </Section>

          <Section title="What Guests Do"
            description="Guests by the kind of role they hold: founders, executives, investors, academics, journalists and officials.">
            <GuestRolesChart />
          </Section>

          <Section title="The Revolving Door"
            description="Where guests used to work and where they work now, by kind of organisation.">
            <RevolvingDoorChart />
          </Section>

          <Section title="Guest Mix by Year"
            description="The share of guest appearances from each kind of organisation, year by year. Most of it barely moves; pick government to see the exception.">
            <GuestMixByYearChart />
          </Section>

          <Section title="Who's On Right Now"
            description="The sixteen guests with the most appearances in the last 18 months, by year. Guests who have appeared on at least four different shows.">
            <GuestMomentumGrid />
          </Section>

          <Section title="Where Each Year's Guests Came From"
            description="Every year's guest roster, split by the year each of those guests first appeared on any show in the network.">
            <GuestCohortChart />
          </Section>

          <Section title="Network Reach"
            description="How many shows each person is one step from: the shows they appear on, plus every show their fellow guests appear on. Ordered by how many shows they appear on themselves.">
            <BridgeChart />
          </Section>

          <Section title="Guest Reach"
            description="Whether a guest turns up once on many shows or returns to the same few. Above the dashed line means they came back.">
            <GuestReachChart />
          </Section>

          <Section title="Guest Appearances"
            description="How concentrated the network is — most people appear once, a few appear dozens of times.">
            <GuestAppearanceChart />
          </Section>
        </>
      ) : view === 'topics' ? (
        <>
          <Section title="What the Network Talks About, Year by Year"
            description="The share of tagged episodes touching each kind of topic, per year. Pick a category to see it on its own scale.">
            <TopicCategoriesByYearChart />
          </Section>

          <Section title="Areas Over Time"
            description="The broad topics within one category, as a share of tagged episodes per year. Pick a category; pick a line to see it on its own scale.">
            <TopicAreasByYearChart />
          </Section>

          <Section title="Rising and Fading Topics"
            description="The broad topics, or individual topics, whose share of episodes grew or shrank the most over the last two years.">
            <RisingTopicsChart />
          </Section>

          <Section title="What Each Show Covers"
            description="Each show's episodes by kind of topic. Click a category to sort shows by it.">
            <ShowTopicMixChart />
          </Section>

          <Section title="Most Discussed"
            description="The companies and people that come up most on episodes they weren't on themselves.">
            <MostDiscussedChart />
          </Section>

          <p className="text-xs text-gray-400">
            Topics are read from episode descriptions, and these charts show shares of the tagged episodes
            (episodes added since the last tagging pass aren't counted yet). See all topics on the <a href="/topics" className="text-teal-700 hover:underline">Topics</a> page.
          </p>
        </>
      ) : (
        <>
          <Section title="Who Each Show Books"
            description="Each show's guests by the kind of organisation they work for. Some shows are industry shows, some are policy or academic ones.">
            <ShowGuestMixChart />
          </Section>

          <Section title="Show Overlap"
            description="How many guests the top 25 most-connected shows share with each other.">
            <ShowOverlapMatrix />
          </Section>

          <Section title="Show Timelines"
            description="When each show started publishing, how often, and whether it's still active.">
            <ShowTimelineGantt />
          </Section>

          <Section title="Episode Activity"
            description="Episodes published per day across the whole network.">
            <EpisodeCalendarHeatmap />
          </Section>
        </>
      )}
    </SiteShell>
  );
}
