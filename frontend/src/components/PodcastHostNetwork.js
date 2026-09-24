import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
// The 2D entry point, not the 'react-force-graph' barrel: that one imports all
// four renderers at module scope, so three.js, A-Frame and AR.js were bundled
// and shipped on every load despite nothing here being able to reach them.
import ForceGraph2D from 'react-force-graph-2d';
import { forceX, forceY, forceCollide, forceManyBody } from 'd3-force';
import { getAdminPassword } from '../adminAuth';
import { API_BASE_URL } from '../config';

// ─── Constants ────────────────────────────────────────────────────────────────

const API_URL = `${API_BASE_URL}/api/host-connections`;
const SHOW_API_URL = `${API_BASE_URL}/api/show-connections`;
const LAST_UPDATED_URL = `${API_BASE_URL}/api/last-updated`;
const SIDEBAR_WIDTH = 384;
const MOBILE_BREAKPOINT = 768;

// Below the mobile breakpoint the sidebar overlays the graph instead of
// pushing it over, so the graph should use the full viewport width.
const getGraphWidth = () =>
  window.innerWidth >= MOBILE_BREAKPOINT ? window.innerWidth - SIDEBAR_WIDTH : window.innerWidth;

// ─── Helpers ──────────────────────────────────────────────────────────────────

// How big a node is drawn. Used by the renderer, the pointer hit area and the
// collision force, which have to agree or nodes overlap despite the force.
const RING_WIDTH = 1.5;
// The constant, not the root, is what sets the size of a small node: at degree
// 2 it is more than half the radius, at degree 217 barely a tenth. Lowering it
// takes 21% off the smallest circles — 61% of the visible graph is degree 4 or
// under — while the biggest hub loses 4%, so the spread between them widens.
const nodeRadius = (node) =>
  node.isShow
    // Shows scale by guest pool, not by edge count: a show's weight in this
    // view is how many people it has had on, and there are only 91 of them,
    // so they can be drawn large enough for the cover art to be recognisable.
    ? Math.max(8, 5 + Math.sqrt(node.people || 1) * 1.6)
    : (node.val <= 1 ? 2.5 : 2.5 + Math.sqrt(node.val) * 2.2);

const getAvatarUrl = (name) =>
  `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(name)}&backgroundColor=65c9ff,92a1c6,dd6b7f,58c9b9,ade498`;

const formatRelativeTime = (isoString) => {
  if (!isoString) return null;
  const then = new Date(isoString.endsWith('Z') ? isoString : isoString + 'Z');
  const seconds = Math.max(0, Math.floor((Date.now() - then.getTime()) / 1000));
  if (seconds < 60) return 'just now';
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes} minute${minutes !== 1 ? 's' : ''} ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours} hour${hours !== 1 ? 's' : ''} ago`;
  const days = Math.floor(hours / 24);
  return `${days} day${days !== 1 ? 's' : ''} ago`;
};

// Stable color per podcast title (deterministic, no random)
const podcastHue = (title) => {
  let hash = 0;
  for (let i = 0; i < title.length; i++) {
    hash = title.charCodeAt(i) + ((hash << 5) - hash);
  }
  return Math.abs(hash) % 360;
};

const podcastColor = (title) => `hsla(${podcastHue(title)}, 65%, 55%, 0.85)`;

// Links take the show's hue too, but paler and more transparent: they sit
// behind the nodes and there are 4,397 of them, so they read as tint rather
// than as another set of saturated marks. 73 hashed hues is far more than
// anyone can tell apart, so colour here says "these strands are the same show"
// at a glance — the legend and hover are what name it.
const linkColor = (title, alpha) => `hsla(${podcastHue(title)}, 50%, 62%, ${alpha})`;

// The API already merges duplicate people and resolves their role; this only
// adds the per-node tallies the force graph needs.
const processNormalised = ({ nodes: rawNodes, links: rawLinks }) => {
  const nodes = new Map(
    rawNodes.map(n => [n.id, { ...n, val: 0, podcasts: new Set() }])
  );
  const links = [];
  rawLinks.forEach(l => {
    const src = nodes.get(l.source);
    const tgt = nodes.get(l.target);
    if (!src || !tgt) return;          // ignore an edge naming an unknown node
    src.podcasts.add(l.podcast); src.val++;
    tgt.podcasts.add(l.podcast); tgt.val++;
    links.push({ source: l.source, target: l.target, value: l.value, podcast: l.podcast });
  });
  return {
    nodes: [...nodes.values()].map(n => ({ ...n, podcasts: [...n.podcasts] })),
    links: fanOutParallelLinks(links),
  };
};

// Process raw API data into graph nodes/links — defined OUTSIDE component
//
// Accepts both shapes the API has served: the current {nodes, links}, and the
// older row-per-edge array. The two deploy separately, so during a rollout the
// page may meet either one.
// 153 pairs of people are joined by more than one link — Stephen Lacey and
// Katherine Hamilton by six, across four different shows — and every one of
// them was drawn along the same straight line, so four working relationships
// looked like one. Spreading them over a range of curvatures fans them into a
// visible bundle. The sign is taken from the lower id so that a link stored
// A->B and one stored B->A bow the same way instead of cancelling out.
const MAX_CURVE = 0.32;
const fanOutParallelLinks = (links) => {
  const byPair = new Map();
  links.forEach(l => {
    const key = l.source < l.target ? `${l.source}|${l.target}` : `${l.target}|${l.source}`;
    if (!byPair.has(key)) byPair.set(key, []);
    byPair.get(key).push(l);
  });
  byPair.forEach(group => {
    if (group.length === 1) { group[0].curvature = 0; return; }
    const half = (group.length - 1) / 2;
    group.forEach((l, i) => {
      const offset = ((i - half) / half) * MAX_CURVE;
      l.curvature = l.source <= l.target ? offset : -offset;
    });
  });
  return links;
};

const processData = (data) => {
  if (data && !Array.isArray(data) && Array.isArray(data.nodes)) {
    return processNormalised(data);
  }
  // Anything else — an error body, a shape we don't know — yields an empty
  // graph rather than throwing in the fetch handler.
  if (!Array.isArray(data)) return { nodes: [], links: [] };

  const nodes = new Map();
  const links = [];

  data.forEach(conn => {
    // Source node
    if (!nodes.has(conn.source_id)) {
      nodes.set(conn.source_id, {
        id: conn.source_id,
        name: conn.source_name,
        image: conn.source_image || null,
        role: conn.source_role || 'Guest',
        channel: conn.source_channel,
        genre: conn.source_genre,
        val: 0,
        podcasts: new Set(),
      });
    }
    nodes.get(conn.source_id).podcasts.add(conn.podcast_title);
    nodes.get(conn.source_id).val++;
    // 65 people host one show and guest on another. Whichever edge happened to
    // be seen first used to decide their role; presenting any show is the more
    // meaningful of the two, so it wins.
    if (conn.source_role === 'Host') nodes.get(conn.source_id).role = 'Host';

    // Target node
    if (!nodes.has(conn.target_id)) {
      nodes.set(conn.target_id, {
        id: conn.target_id,
        name: conn.target_name,
        image: conn.target_image || null,
        role: conn.target_role || 'Guest',
        channel: conn.target_channel,
        genre: conn.target_genre,
        val: 0,
        podcasts: new Set(),
      });
    }
    nodes.get(conn.target_id).podcasts.add(conn.podcast_title);
    nodes.get(conn.target_id).val++;
    if (conn.target_role === 'Host') nodes.get(conn.target_id).role = 'Host';

    links.push({
      source: conn.source_id,
      target: conn.target_id,
      value: conn.episodes_together,
      podcast: conn.podcast_title,
    });
  });

  // Convert Sets to Arrays
  nodes.forEach(node => { node.podcasts = Array.from(node.podcasts); });

  return { nodes: Array.from(nodes.values()), links: fanOutParallelLinks(links) };
};

// ─── Connected component filter ───────────────────────────────────────────────
// Remove small isolated clusters (fewer than MIN_CLUSTER_SIZE nodes)

const filterSmallClusters = ({ nodes, links }, minSize = 8) => {
  if (!nodes.length) return { nodes, links };

  // Build adjacency map
  const neighbors = new Map();
  nodes.forEach(n => neighbors.set(n.id, new Set()));
  links.forEach(l => {
    const src = l.source?.id ?? l.source;
    const tgt = l.target?.id ?? l.target;
    if (neighbors.has(src)) neighbors.get(src).add(tgt);
    if (neighbors.has(tgt)) neighbors.get(tgt).add(src);
  });

  // BFS to find all connected components
  const visited = new Set();
  const components = [];

  nodes.forEach(node => {
    if (visited.has(node.id)) return;
    const component = [];
    const queue = [node.id];
    while (queue.length) {
      const id = queue.shift();
      if (visited.has(id)) continue;
      visited.add(id);
      component.push(id);
      (neighbors.get(id) || new Set()).forEach(nid => {
        if (!visited.has(nid)) queue.push(nid);
      });
    }
    components.push(component);
  });

  // Keep only nodes in components >= MIN_CLUSTER_SIZE
  const keepIds = new Set(
    components.filter(c => c.length >= minSize).flat()
  );

  const filteredNodes = nodes.filter(n => keepIds.has(n.id));
  const filteredLinks = links.filter(l => {
    const src = l.source?.id ?? l.source;
    const tgt = l.target?.id ?? l.target;
    return keepIds.has(src) && keepIds.has(tgt);
  });

  return { nodes: filteredNodes, links: filteredLinks };
};

// ─── Image cache ──────────────────────────────────────────────────────────────
// Pre-load images once so nodeCanvasObject never creates Image objects at 60fps

const useImageCache = (nodes) => {
  const cache = useRef({});

  useEffect(() => {
    nodes.forEach(node => {
      if (!node.image) return;
      // Proxy external images (unavatar, etc.) to avoid CORS in canvas
      const src = node.image.startsWith('http') && !node.image.includes('mzstatic.com')
        ? `${API_BASE_URL}/api/proxy/image?url=${encodeURIComponent(node.image)}`
        : node.image;
      if (!cache.current[src]) {
        const img = new Image();
        img.crossOrigin = 'anonymous';
        img.src = src;
        img.onload = () => { cache.current[src] = img; };
        cache.current[src] = img;
        // Also store under original URL so nodeCanvasObject can find it
        cache.current[node.image] = img;
      }
    });
  }, [nodes]);

  return cache;
};

// ─── Sub-components ───────────────────────────────────────────────────────────

const CloseButton = ({ onClick }) => (
  <button
    onClick={onClick}
    className="absolute top-2 right-2 w-8 h-8 flex items-center justify-center rounded-full hover:bg-gray-100"
  >
    <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
    </svg>
  </button>
);

// Overlap percentages run 0-35 and the threshold is a whole number, so
// rounding to integers made the card contradict itself: 5.54% displayed as
// "6%" while the >= 6 filter correctly excluded it, leaving "no show shares 6%"
// directly above "its closest is 6%". One decimal, with a bare integer when
// there is no fraction to show.
const overlapPct = (jaccard) => {
  const v = jaccard * 100;
  return Number.isInteger(Math.round(v * 10) / 10) ? v.toFixed(0) : v.toFixed(1);
};

const ShowProfileCard = ({ show, connections, onClose, weighting, allLinks, nameById, threshold }) => {
  const sorted = [...connections].sort((a, b) =>
    weighting === 'jaccard' ? b.jaccard - a.jaccard : b.value - a.value);

  // When the threshold has cut everything, say so — and say what the closest
  // relationship actually is. "0 connected shows" on its own reads as missing
  // data rather than a slider set high.
  const best = connections.length ? null : allLinks
    .filter(l => (l.source?.id ?? l.source) === show.id || (l.target?.id ?? l.target) === show.id)
    .map(l => {
      const otherId = (l.source?.id ?? l.source) === show.id
        ? (l.target?.id ?? l.target) : (l.source?.id ?? l.source);
      return { name: nameById.get(otherId), value: l.value, jaccard: l.jaccard };
    })
    .sort((a, b) => (weighting === 'jaccard' ? b.jaccard - a.jaccard : b.value - a.value))[0];
  return (
    <div className="bg-white rounded-lg shadow-lg p-6 relative">
      <CloseButton onClick={onClose} />
      <div className="flex flex-col items-center mb-6">
        <div className="w-24 h-24 rounded-xl overflow-hidden mb-3 bg-gray-100">
          <img src={show.image || getAvatarUrl(show.name)} alt={show.name}
               className="w-full h-full object-cover"
               onError={e => { e.target.onerror = null; e.target.src = getAvatarUrl(show.name); }} />
        </div>
        <h3 className="text-xl font-bold text-center leading-tight">{show.name}</h3>
        <p className="text-gray-500 text-sm mt-1">
          {connections.length} connected show{connections.length === 1 ? '' : 's'}
        </p>
      </div>

      <div className="grid grid-cols-2 gap-2 text-sm mb-6">
        <div className="bg-teal-50 p-2 rounded text-center">
          <p className="text-gray-500 text-xs">Episodes</p>
          <p className="font-bold">{show.episodes}</p>
        </div>
        <div className="bg-teal-50 p-2 rounded text-center">
          <p className="text-gray-500 text-xs">People</p>
          <p className="font-bold">{show.people}</p>
        </div>
      </div>

      {connections.length === 0 ? (
        <div className="text-sm text-gray-500 space-y-2">
          <p>
            {weighting === 'jaccard'
              ? `No other show shares ${threshold}% of its guests with this one.`
              : `No other show has ${threshold} people in common with this one.`}
          </p>
          {best ? (
            <p>
              Its closest is <span className="font-medium text-gray-700">{best.name}</span> —
              {' '}{best.value} {best.value === 1 ? 'person' : 'people'} in common,
              {' '}{overlapPct(best.jaccard)}% of their combined guests.
              Lower the threshold to bring it in.
            </p>
          ) : (
            <p>It shares nobody with any other show in the network.</p>
          )}
        </div>
      ) : (
      <div>
        <h4 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-2">
          Shares Most People With
        </h4>
        <div className="space-y-2">
          {sorted.slice(0, 6).map((c, i) => {
            const other = c.source.id === show.id ? c.target : c.source;
            return (
              <div key={i} className="bg-gray-50 p-2 rounded text-sm">
                <p className="font-medium leading-tight">{other.name}</p>
                <p className="text-gray-500 text-xs">
                  {c.value} in common · {overlapPct(c.jaccard)}% of their combined guests
                </p>
              </div>
            );
          })}
        </div>
      </div>
      )}
    </div>
  );
};

// The one current job title / organisation for a person — derived from
// episode text or pinned by hand (see backend/role_selection.py). Fetched per
// card rather than carried in the graph payload, which stays unchanged.
const useCurrentRole = (hostId) => {
  const [role, setRole] = useState(null);
  useEffect(() => {
    setRole(null);
    if (hostId == null) return undefined;
    let cancelled = false;
    fetch(`${API_BASE_URL}/api/people/${hostId}/current-role`)
      .then(r => (r.ok ? r.json() : null))
      .then(d => { if (!cancelled) setRole(d?.current_role || null); })
      .catch(() => {});   // a missing role just means no line on the card
    return () => { cancelled = true; };
  }, [hostId]);
  return role;
};

const HostProfileCard = ({ host, connections, onClose, isAdmin }) => {
  const uniquePodcasts = new Set(connections.map(c => c.podcast)).size;
  const totalEpisodes = connections.reduce((s, c) => s + c.value, 0);
  const role = useCurrentRole(host.id);

  return (
    <div className="bg-white rounded-lg shadow-lg p-6 relative">
      <CloseButton onClick={onClose} />
      <div className="flex flex-col items-center mb-6">
        <div className="w-24 h-24 rounded-full p-0.5 mb-3" style={{ backgroundColor: '#94a3b8' }}>
          <div className="w-full h-full rounded-full overflow-hidden bg-white">
            <img
              src={host.image || getAvatarUrl(host.name)}
              alt={host.name}
              className="w-full h-full object-cover"
              onError={e => { e.target.onerror = null; e.target.src = getAvatarUrl(host.name); }}
            />
          </div>
        </div>
        {isAdmin ? (
          <a href={`/admin/people?host_id=${host.id}`}
             className="text-xl font-bold text-center hover:text-teal-600 hover:underline">
            {host.name}
          </a>
        ) : (
          <h3 className="text-xl font-bold text-center">{host.name}</h3>
        )}
        {role && (role.title || role.company) && (
          <p className="text-sm text-gray-700 text-center mt-1">
            {role.title}{role.title && role.company ? ' · ' : ''}{role.company}
          </p>
        )}
        <p className="text-gray-500 text-sm">{connections.length} connections</p>
        {host.linkedin_url && (
          <a
            href={host.linkedin_url}
            target="_blank"
            rel="noopener noreferrer"
            className="mt-2 text-xs text-teal-600 hover:underline"
          >
            LinkedIn →
          </a>
        )}
      </div>

      <div className="mb-4">
        <h4 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-2">Stats</h4>
        <div className="grid grid-cols-3 gap-2 text-sm">
          <div className="bg-teal-50 p-2 rounded text-center">
            <p className="text-gray-500 text-xs">Podcasts</p>
            <p className="font-bold">{uniquePodcasts}</p>
          </div>
          <div className="bg-teal-50 p-2 rounded text-center">
            <p className="text-gray-500 text-xs">Episodes</p>
            <p className="font-bold">{totalEpisodes}</p>
          </div>
          <div className="bg-teal-50 p-2 rounded text-center">
            <p className="text-gray-500 text-xs">Connects</p>
            <p className="font-bold">{host.val}</p>
          </div>
        </div>
      </div>

      <div>
        <h4 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-2">Appeared Most With</h4>
        <div className="space-y-2">
          {connections
            .sort((a, b) => b.value - a.value)
            .slice(0, 5)
            .map((conn, idx) => {
              // By id, not by name: two people can share a name, and this app
              // has spent a lot of time untangling exactly that.
              const other = conn.source.id === host.id ? conn.target : conn.source;
              return (
                <div key={idx} className="bg-gray-50 p-2 rounded text-sm">
                  {isAdmin ? (
                    <a href={`/admin/people?host_id=${other.id}`}
                       className="font-medium hover:text-teal-600 hover:underline">
                      {other.name}
                    </a>
                  ) : (
                    <p className="font-medium">{other.name}</p>
                  )}
                  <p className="text-gray-500 text-xs">
                    {conn.value} episode{conn.value !== 1 ? 's' : ''} together on {conn.podcast}
                  </p>
                </div>
              );
            })}
        </div>
      </div>
    </div>
  );
};

const ConnectionDetails = ({ connection, onClose }) => (
  <div className="bg-white rounded-lg shadow-lg p-6 relative">
    <CloseButton onClick={onClose} />
    <h3 className="text-xl font-bold mb-4">Connection Details</h3>
    <div className="space-y-4">
      <div>
        <h4 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-2">People</h4>
        <div className="bg-teal-50 p-3 rounded space-y-2">
          <p className="font-medium">{connection.source.name}</p>
          <div className="flex items-center">
            <div className="flex-1 border-t border-gray-300" />
            <span className="px-2 text-gray-400 text-sm">with</span>
            <div className="flex-1 border-t border-gray-300" />
          </div>
          <p className="font-medium">{connection.target.name}</p>
        </div>
      </div>
      <div>
        <h4 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-2">Collaboration</h4>
        <div className="bg-teal-50 p-3 rounded">
          <p className="text-sm">
            <span className="font-bold">{connection.value}</span> episode{connection.value !== 1 ? 's' : ''} together
          </p>
          <p className="text-sm text-gray-500 mt-1">on <span className="font-medium">{connection.podcast}</span></p>
        </div>
      </div>
    </div>
  </div>
);

const FilterPanel = ({ onFiltersChange, networkStats, currentFilters, searchQuery, onSearchChange, loading, isAdmin, viewMode, onViewModeChange, showGraphError }) => (
  <div className="space-y-4">
    <div className="flex rounded-lg bg-gray-100 p-0.5 text-sm">
      {[['people', 'People'], ['shows', 'Shows']].map(([mode, label]) => (
        <button
          key={mode}
          onClick={() => onViewModeChange(mode)}
          className={`flex-1 rounded-md py-1.5 font-medium transition-colors ${
            viewMode === mode ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500 hover:text-gray-700'}`}
        >
          {label}
        </button>
      ))}
    </div>

    {showGraphError && viewMode === 'shows' && (
      <p className="text-sm text-red-500">Couldn't load the show network: {showGraphError}</p>
    )}

    <div className={`grid gap-2 ${viewMode === 'shows' ? 'grid-cols-2' : 'grid-cols-3'}`}>
      {(viewMode === 'shows'
        // Counting people here would sum each show's guest list and double
        // count everyone who appears on more than one, which is the whole
        // subject of the view.
        ? [
            { label: 'Podcasts', value: networkStats.visiblePodcasts },
            { label: 'Connections', value: networkStats.visibleLinks },
          ]
        : [
            // "People", not "Hosts": 1,825 of the 1,952 in the graph appear
            // only as guests.
            { label: 'Podcasts', value: networkStats.visiblePodcasts },
            { label: 'People', value: networkStats.visibleNodes },
            { label: 'Connections', value: networkStats.visibleLinks },
          ]
      ).map(({ label, value }) => (
        <div key={label} className="bg-teal-50 rounded-lg text-center py-2">
          <p className="text-lg font-bold text-gray-900 leading-tight">{loading ? ' ' : value}</p>
          <p className="text-xs text-gray-500">{label}</p>
        </div>
      ))}
    </div>

    {/* Name search — people only. 91 shows are all on screen at once and
        carry their own artwork, so there is nothing to hunt for. */}
    {viewMode !== 'shows' && (
    <div className="space-y-1">
      <label className="block text-sm font-medium text-gray-700">Search by name</label>
      <input
        type="text"
        value={searchQuery}
        onChange={e => onSearchChange(e.target.value)}
        placeholder="e.g. Jigar Shah"
        className="w-full rounded-md border border-gray-300 px-3 py-1.5 text-sm shadow-sm focus:border-teal-500 focus:outline-none focus:ring-1 focus:ring-teal-500"
      />
    </div>
    )}

    {viewMode === 'shows' ? (
      currentFilters.weighting === 'jaccard' ? (
      <div className="space-y-1">
        <label className="block text-sm font-medium text-gray-700">
          Overlap: {currentFilters.minOverlap}%
        </label>
        <input
          type="range" min="2" max="25"
          value={currentFilters.minOverlap}
          onChange={e => onFiltersChange({ minOverlap: parseInt(e.target.value) })}
          className="w-full"
        />
        <div className="flex justify-between text-xs text-gray-400">
          <span>2%</span><span>25%</span>
        </div>
        <p className="text-xs text-gray-400">
          Share of two shows' combined guests that appear on both. Around 10%
          the network separates into the solar trade, the policy press, the
          international circuit and the nuclear pair.
        </p>
      </div>
      ) : (
      <div className="space-y-1">
        <label className="block text-sm font-medium text-gray-700">
          People in Common: {currentFilters.minShared}
        </label>
        <input
          type="range" min="1" max="20"
          value={currentFilters.minShared}
          onChange={e => onFiltersChange({ minShared: parseInt(e.target.value) })}
          className="w-full"
        />
        <div className="flex justify-between text-xs text-gray-400">
          <span>1</span><span>20</span>
        </div>
        <p className="text-xs text-gray-400">
          How many people two shows must both have had on. Raw counts favour
          the biggest shows, which connect to everything — this stays a single
          mass at any threshold.
        </p>
      </div>
      )
    ) : (
    <>
    {/* Connections slider */}
    <div className="space-y-1">
      <label className="block text-sm font-medium text-gray-700">
        Minimum Connections: {currentFilters.minConnections}
      </label>
      <input
        type="range" min="1" max={Math.max(1, networkStats.maxConnections)}
        value={currentFilters.minConnections}
        onChange={e => onFiltersChange({ minConnections: parseInt(e.target.value) })}
        className="w-full"
      />
      <div className="flex justify-between text-xs text-gray-400">
        <span>1</span><span>{networkStats.maxConnections}</span>
      </div>
    </div>

    {/* Podcasts slider */}
    <div className="space-y-1">
      <label className="block text-sm font-medium text-gray-700">
        Minimum Podcasts: {currentFilters.minPodcasts}
      </label>
      <input
        type="range" min="1" max={Math.max(1, networkStats.maxPodcasts)}
        value={currentFilters.minPodcasts}
        onChange={e => onFiltersChange({ minPodcasts: parseInt(e.target.value) })}
        className="w-full"
      />
      <div className="flex justify-between text-xs text-gray-400">
        <span>1</span><span>{networkStats.maxPodcasts}</span>
      </div>
    </div>


    {/* Roles */}
    <div className="flex items-center gap-4">
      <label className="text-sm font-medium text-gray-700">Roles</label>
      {['Host', 'Guest'].map(role => (
        <label key={role} className="flex items-center gap-1.5">
          <input
            type="checkbox"
            checked={currentFilters.selectedRoles.includes(role)}
            onChange={e => {
              const newRoles = e.target.checked
                ? [...currentFilters.selectedRoles, role]
                : currentFilters.selectedRoles.filter(r => r !== role);
              onFiltersChange({ selectedRoles: newRoles });
            }}
            className="rounded text-teal-600"
          />
          <span className="text-sm text-gray-700">{role}</span>
        </label>
      ))}
    </div>
    </>
    )}

    <button
      onClick={() => onFiltersChange({
        minConnections: 2, minPodcasts: 1,
        minEpisodes: 1, minShared: 4, minOverlap: 5, weighting: 'jaccard',
        minClusterSize: 8, repulsion: 60, centering: 8, spacing: 1,
        drawMinEpisodes: 1,
        selectedRoles: ['Host', 'Guest'],
        selectedChannel: 'all', selectedGenre: 'all',
      })}
      className="w-full py-2 px-4 bg-gray-100 hover:bg-gray-200 rounded-md text-sm font-medium text-gray-600"
    >
      Reset Filters
    </button>

    {/* Everything below Reset is for tuning the graph, not exploring it, so the
        public panel and the admin panel are the same view plus a tail. */}
    {isAdmin && (
    <div className="space-y-4 border-t border-gray-200 pt-4">
      <p className="text-xs font-semibold uppercase tracking-wide text-gray-400">Admin</p>

      {viewMode === 'shows' && (
        <div className="space-y-1">
          <label className="block text-sm font-medium text-gray-700">Edge weight</label>
          <div className="flex rounded-md bg-gray-100 p-0.5 text-xs">
            {[['raw', 'Shared people'], ['jaccard', 'Normalised']].map(([w, label]) => (
              <button
                key={w}
                onClick={() => onFiltersChange({ weighting: w })}
                className={`flex-1 rounded py-1 font-medium ${
                  currentFilters.weighting === w ? 'bg-white text-gray-900 shadow-sm' : 'text-gray-500'}`}
              >
                {label}
              </button>
            ))}
          </div>
          <p className="text-xs text-gray-400">
            Raw counts favour big shows — Volts and Inevitable share 44 mostly
            because both have had ~200 guests. Normalised divides by their
            combined pool, which surfaces Nuclear Barbarians and Titans Of
            Nuclear: 6 people, but 30% of everyone either has ever had on.
          </p>
        </div>
      )}

      {/* Genre and Channel are podcast properties shown against people, so a
          person's genre is whichever show happened to be listed first. Hidden
          until that is fixed. */}
      <div className="grid grid-cols-2 gap-3">
        <div className="space-y-1">
          <label className="block text-sm font-medium text-gray-700">Genre</label>
          <select
            value={currentFilters.selectedGenre}
            onChange={e => onFiltersChange({ selectedGenre: e.target.value })}
            className="w-full rounded-md border border-gray-300 px-2 py-1.5 text-sm shadow-sm focus:border-teal-500 focus:outline-none"
          >
            <option value="all">All Genres</option>
            {networkStats.genres?.map(g => <option key={g} value={g}>{g}</option>)}
          </select>
        </div>

        <div className="space-y-1">
          <label className="block text-sm font-medium text-gray-700">Channel</label>
          <select
            value={currentFilters.selectedChannel}
            onChange={e => onFiltersChange({ selectedChannel: e.target.value })}
            className="w-full rounded-md border border-gray-300 px-2 py-1.5 text-sm shadow-sm focus:border-teal-500 focus:outline-none"
          >
            <option value="all">All Channels</option>
            {networkStats.channels?.map(c => <option key={c} value={c}>{c}</option>)}
          </select>
        </div>
      </div>

      {/* Min episodes per connection — filters relationships, not people */}
      <div className="space-y-1">
        <label className="block text-sm font-medium text-gray-700">
          Minimum Episodes Together: {currentFilters.minEpisodes}
        </label>
        <input
          type="range" min="1" max="10"
          value={currentFilters.minEpisodes}
          onChange={e => onFiltersChange({ minEpisodes: parseInt(e.target.value) })}
          className="w-full"
        />
        <div className="flex justify-between text-xs text-gray-400">
          <span>1</span><span>10</span>
        </div>
        <p className="text-xs text-gray-400">
          82% of connections are a single shared episode. Raising this leaves only
          recurring working relationships, which pulls the dense middle apart.
        </p>
      </div>

      <div className="space-y-1">
        <label className="block text-sm font-medium text-gray-700">
          Draw Links From: {currentFilters.drawMinEpisodes} episode{currentFilters.drawMinEpisodes === 1 ? '' : 's'}
        </label>
        <input
          type="range" min="1" max="10"
          value={currentFilters.drawMinEpisodes}
          onChange={e => onFiltersChange({ drawMinEpisodes: parseInt(e.target.value) })}
          className="w-full"
        />
        <div className="flex justify-between text-xs text-gray-400">
          <span>1</span><span>10</span>
        </div>
        <p className="text-xs text-gray-400">
          Hides weak links from view only. They keep pulling, so nobody moves —
          the same layout with the noise taken out. The slider above removes
          them from the layout too, which rearranges everything.
        </p>
      </div>

      <div className="space-y-1">
        <label className="block text-sm font-medium text-gray-700">
          Minimum Cluster Size: {currentFilters.minClusterSize}
        </label>
        <input
          type="range" min="1" max="20"
          value={currentFilters.minClusterSize}
          onChange={e => onFiltersChange({ minClusterSize: parseInt(e.target.value) })}
          className="w-full"
        />
        <div className="flex justify-between text-xs text-gray-400">
          <span>1</span><span>20</span>
        </div>
      </div>

      <div className="space-y-1">
        <label className="block text-sm font-medium text-gray-700">
          Repulsion: {currentFilters.repulsion}
        </label>
        <input
          type="range" min="10" max="400" step="10"
          value={currentFilters.repulsion}
          onChange={e => onFiltersChange({ repulsion: parseInt(e.target.value) })}
          className="w-full"
        />
        <div className="flex justify-between text-xs text-gray-400">
          <span>10</span><span>400</span>
        </div>
        <p className="text-xs text-gray-400">
          How hard nodes push each other apart. Re-runs the layout. At 30 the
          centre is 77% covered in circles; 60 halves that, 180 reaches 15% but
          spreads the graph twice as wide.
        </p>
      </div>

      <div className="space-y-1">
        <label className="block text-sm font-medium text-gray-700">
          Centering: {currentFilters.centering}
        </label>
        <input
          type="range" min="1" max="20"
          value={currentFilters.centering}
          onChange={e => onFiltersChange({ centering: parseInt(e.target.value) })}
          className="w-full"
        />
        <div className="flex justify-between text-xs text-gray-400">
          <span>1</span><span>20</span>
        </div>
        <p className="text-xs text-gray-400">
          How hard everything is pulled toward the middle. The only dial that
          improves two things at once: at 4 the shows separate better than at 8
          and the centre is less crowded, in exchange for a wider graph.
        </p>
      </div>

      <div className="space-y-1">
        <label className="block text-sm font-medium text-gray-700">
          Node Spacing: {currentFilters.spacing}
        </label>
        <input
          type="range" min="0" max="10"
          value={currentFilters.spacing}
          onChange={e => onFiltersChange({ spacing: parseInt(e.target.value) })}
          className="w-full"
        />
        <div className="flex justify-between text-xs text-gray-400">
          <span>0</span><span>10</span>
        </div>
        <p className="text-xs text-gray-400">
          Clear space kept around every node. 0 lets them touch; past about 4 it
          stops separating and starts imposing even spacing, which flattens the
          density differences that show who is loosely connected.
        </p>
      </div>
    </div>
    )}
  </div>
);

const PodcastLegend = ({ podcasts, isOpen, onToggle, selected, onSelect }) => (
  <div className="absolute bottom-4 right-4 bg-white rounded-lg shadow-lg max-w-xs">
    <button
      onClick={onToggle}
      className="w-full flex items-center justify-between px-4 py-2 text-sm font-semibold text-gray-700"
    >
      <span>Podcasts ({podcasts.length})</span>
      <span>{isOpen ? '▲' : '▼'}</span>
    </button>
    {isOpen && (
      <div className="px-4 pb-3 space-y-0.5 max-h-64 overflow-y-auto">
        {selected && (
          <button
            onClick={() => onSelect(null)}
            className="w-full text-left text-xs text-teal-700 hover:text-teal-900 py-1"
          >
            ← show everything
          </button>
        )}
        {podcasts.map(p => (
          <button
            key={p}
            onClick={() => onSelect(selected === p ? null : p)}
            className={`w-full flex items-center text-xs text-left rounded px-1 py-0.5 ${
              selected === p ? 'bg-amber-50 font-semibold text-gray-900' : 'hover:bg-gray-50'}`}
          >
            <div className="w-3 h-3 rounded-full mr-2 flex-shrink-0"
                 style={{ backgroundColor: podcastColor(p) }} />
            <span className="truncate">{p}</span>
          </button>
        ))}
      </div>
    )}
  </div>
);

// ─── Main component ───────────────────────────────────────────────────────────

const PodcastHostNetwork = () => {
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  // The same network one level up: shows as nodes, shared people as edges.
  // Fetched lazily the first time it is asked for — most visits never switch.
  // ?view=shows survives a refresh and makes either view linkable.
  const [viewMode, setViewMode] = useState(() => {
    try {
      return new URLSearchParams(window.location.search).get('view') === 'shows'
        ? 'shows' : 'people';
    } catch {
      return 'people';   // URLSearchParams is absent in some embedded webviews
    }
  });
  const [showGraph, setShowGraph] = useState(null);
  const [showGraphError, setShowGraphError] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [highlightNodes, setHighlightNodes] = useState(new Set());
  const [highlightLinks, setHighlightLinks] = useState(new Set());
  const [selectedNode, setSelectedNode] = useState(null);
  const [selectedLink, setSelectedLink] = useState(null);
  const [selectedNodeConnections, setSelectedNodeConnections] = useState([]);
  const [selectedLinks, setSelectedLinks] = useState(new Set());
  const [selectedPodcast, setSelectedPodcast] = useState(null);
  const [legendOpen, setLegendOpen] = useState(true);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [lastUpdated, setLastUpdated] = useState(null);
  const [dimensions, setDimensions] = useState({
    width: getGraphWidth(),
    height: window.innerHeight,
  });
  const [currentFilters, setCurrentFilters] = useState({
    minConnections: 2,
    minPodcasts: 1,
    minEpisodes: 1,
    minShared: 4,          // shows view, raw weighting: people two shows must have in common
    minOverlap: 5,         // shows view, normalised: shared share of the combined pool, in %
    // Normalised by default. Raw counts are the more intuitive number but they
    // produce a single mass at every threshold — the biggest shows connect to
    // everything, so a raw floor keeps exactly the edges carrying no
    // information. Dividing by the combined guest pool is what separates the
    // solar trade from the policy press from the nuclear pair.
    weighting: 'jaccard',
    minClusterSize: 8,
    repulsion: 60,
    drawMinEpisodes: 1,
    centering: 8,   // pull toward the middle, as strength x100
    spacing: 1,     // extra clear space the collision force keeps, in units
    selectedRoles: ['Host', 'Guest'],
    selectedChannel: 'all',
    selectedGenre: 'all',
  });
  const [networkStats, setNetworkStats] = useState({
    maxConnections: 0, maxPodcasts: 0,
    channels: [], genres: [],
    visibleNodes: 0, visibleLinks: 0, visiblePodcasts: 0,
  });

  // Image cache — pre-loads all node images once
  const graphRef = useRef(null);
  const hasAutoFitted = useRef(false);
  const repulsionRef = useRef(60);
  const centeringRef = useRef(8);
  const spacingRef = useRef(1);

  // Admin is localStorage-only and unverified until the server rejects it, so
  // this reveals controls and nothing more — never anything worth protecting.
  const isAdmin = useMemo(() => !!getAdminPassword(), []);

  // Installed via the ref rather than on the first engine tick: a tick-1 install
  // means tick 0 lays out under different forces and then visibly reorganises.
  // force-graph keeps one simulation across data changes, so this runs once.
  const installForces = useCallback(graph => {
    graphRef.current = graph;
    if (!graph || graph._forcesSet) return;
    // Repulsion. force-graph builds forceManyBody() unconfigured, so this ran
    // at d3's default of -30 — far too weak for 1,192 nodes, which is why the
    // centre packed into an unreadable mass: circles covered 77% of the central
    // disc, near-tiling it. The coverage figure is zoom-independent, so
    // lowering it thins the middle out rather than just drawing it larger.
    //
    // -60 halves that to 42%. The dead d3ForceStrength prop asked for -180,
    // which reaches 15% but flings the graph into a sparse web 2.4x wider —
    // legible, and much less like a network.
    graph.d3Force('charge', forceManyBody().strength(-repulsionRef.current));
    graph.d3Force('x', forceX(0).strength(centeringRef.current / 100));
    graph.d3Force('y', forceY(0).strength(centeringRef.current / 100));
    // Nothing previously kept two nodes from occupying the same point, so the
    // default view settled with 107 pairs permanently overlapping, the worst
    // 60% buried. Radius is what gets drawn plus the ring, and 1u of margin.
    //
    // Keep that margin small. Uneven density is signal here — it is what shows
    // a group as poorly connected to the rest — and a wider collision radius
    // erases it. At 6u the clear space inside the dense core more than doubles
    // (5.5u to 11.9u, inflating exactly the clusters that should read as
    // tight) and the spread of spacing around the outer nodes falls from 0.93
    // to 0.74, flattening the periphery into an even ring. At 1u both match an
    // uncollided layout to two decimal places, and no pair overlaps.
    graph.d3Force('collide',
      forceCollide(node => nodeRadius(node) + RING_WIDTH + spacingRef.current).iterations(2));
    graph._forcesSet = true;
  }, []);
  // Whichever set is on screen. Pointing this at graphData only meant show
  // cover art was never preloaded, so the painter found an empty cache and
  // every show fell back to initials.
  const imageCache = useImageCache(
    viewMode === 'shows' ? (showGraph?.nodes || []) : graphData.nodes
  );

  // Window resize
  useEffect(() => {
    const onResize = () => setDimensions({
      width: getGraphWidth(),
      height: window.innerHeight,
    });
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, []);

  // Data fetch
  const loadData = useCallback(() => {
    setLoading(true);
    setError(null);
    fetch(API_URL)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      // processData handles both the {nodes, links} object and the older
      // row-per-edge array. Coercing a non-array to [] here threw the object
      // away before it ever reached that check, which emptied the graph.
      .then(data => setGraphData(processData(data || [])))
      .catch(err => setError(err.message || 'Failed to load network data'))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  useEffect(() => {
    if (viewMode !== 'shows' || showGraph) return;
    fetch(SHOW_API_URL)
      .then(r => { if (!r.ok) throw new Error(`API error ${r.status}`); return r.json(); })
      .then(d => setShowGraph({
        nodes: (d.nodes || []).map(n => ({ ...n, isShow: true })),
        links: (d.links || []).map(l => ({ ...l, jaccard: Number(l.jaccard), isShowLink: true })),
      }))
      .catch(e => setShowGraphError(e.message || 'Failed to load the show network'));
  }, [viewMode, showGraph]);

  // Last-updated timestamp — fetched once, then re-rendered periodically
  // so the relative time ("3 hours ago") stays fresh without a refetch
  useEffect(() => {
    fetch(LAST_UPDATED_URL)
      .then(r => r.ok ? r.json() : null)
      .then(data => data?.last_run_at && setLastUpdated(data.last_run_at))
      .catch(() => {});
  }, []);

  const [, forceTick] = useState(0);
  useEffect(() => {
    const interval = setInterval(() => forceTick(t => t + 1), 60000);
    return () => clearInterval(interval);
  }, []);



  // Compute network stats when base data loads
  useEffect(() => {
    if (!graphData.nodes.length) return;
    const maxConnections = Math.max(...graphData.nodes.map(n => n.val));
    const maxPodcasts = Math.max(...graphData.nodes.map(n => n.podcasts.length));
    const channels = [...new Set(graphData.nodes.map(n => n.channel).filter(Boolean))].sort();
    const genres = [...new Set(graphData.nodes.map(n => n.genre).filter(Boolean))].sort();
    setNetworkStats(prev => ({ ...prev, maxConnections, maxPodcasts, channels, genres }));
  }, [graphData]);

  // Repulsion is the one filter that changes the simulation rather than the
  // data, so it is applied to the live force and the layout reheated. The fit
  // is re-armed because the graph's extent changes a lot with it.
  const { repulsion, centering, spacing } = currentFilters;
  useEffect(() => {
    repulsionRef.current = repulsion;
    centeringRef.current = centering;
    spacingRef.current = spacing;
    const graph = graphRef.current;
    if (!graph?.d3Force) return;
    graph.d3Force('charge', forceManyBody().strength(-repulsion));
    graph.d3Force('x', forceX(0).strength(centering / 100));
    graph.d3Force('y', forceY(0).strength(centering / 100));
    graph.d3Force('collide',
      forceCollide(node => nodeRadius(node) + RING_WIDTH + spacing).iterations(2));
    hasAutoFitted.current = false;
    graph.d3ReheatSimulation();
  }, [repulsion, centering, spacing]);

  // Filtered graph — recomputed when filters or data change
  const filteredGraphData = useMemo(() => {
    if (!graphData.nodes.length) return { nodes: [], links: [] };

    const q = searchQuery.toLowerCase().trim();

    const filteredNodes = graphData.nodes.filter(node => {
      if (q && !node.name.toLowerCase().includes(q)) return false;
      if (node.val < currentFilters.minConnections) return false;
      if (node.podcasts.length < currentFilters.minPodcasts) return false;
      if (currentFilters.selectedRoles.length && !currentFilters.selectedRoles.includes(node.role)) return false;
      if (currentFilters.selectedGenre !== 'all' && node.genre !== currentFilters.selectedGenre) return false;
      if (currentFilters.selectedChannel !== 'all' && node.channel !== currentFilters.selectedChannel) return false;
      return true;
    });

    const validIds = new Set(filteredNodes.map(n => n.id));
    const filteredLinks = graphData.links.filter(l => {
      // Weak ties are the webbing that holds the middle together: 82% of links
      // are one shared episode, and each pulls as hard as a 350-episode
      // co-hosting bond. Dropping them is the only thing that separates the
      // clusters — no force setting comes close.
      if (l.value < currentFilters.minEpisodes) return false;
      const src = l.source?.id ?? l.source;
      const tgt = l.target?.id ?? l.target;
      return validIds.has(src) && validIds.has(tgt);
    });

    return filterSmallClusters({ nodes: filteredNodes, links: filteredLinks }, currentFilters.minClusterSize);
  }, [graphData, currentFilters, searchQuery]);

  // The shows view. Far smaller — 91 nodes against 1,952 — so it needs no
  // cluster pruning or degree floor, just a threshold on how much two shows
  // have to have in common before a line is worth drawing.
  const filteredShowData = useMemo(() => {
    if (!showGraph) return { nodes: [], links: [] };
    // Deliberately ignores searchQuery: the box is hidden in this view, so a
    // term left over from the people graph would filter shows invisibly with
    // no control on screen to clear it.
    const nodes = showGraph.nodes;
    const ids = new Set(nodes.map(n => n.id));

    // The threshold is always on the raw count, even when the width is drawn
    // from jaccard: a pair sharing one person out of two tiny pools scores
    // 0.17 and would otherwise outrank real relationships.
    // The threshold follows the weighting. Cutting on the raw count while
    // drawing normalised widths is what left the graph a blob: big shows
    // connect to everything, so a raw floor keeps exactly the edges that carry
    // no information. Cutting on overlap instead separates it into the solar
    // trade, the policy press, the international circuit and the nuclear pair.
    const normalised = currentFilters.weighting === 'jaccard';
    const links = showGraph.links.filter(l => {
      if (normalised) {
        // A raw floor still applies: one shared person between two small shows
        // scores 17% and would otherwise outrank real relationships.
        if (l.value < 3 || l.jaccard * 100 < currentFilters.minOverlap) return false;
      } else if (l.value < currentFilters.minShared) {
        return false;
      }
      const src = l.source?.id ?? l.source;
      const tgt = l.target?.id ?? l.target;
      return ids.has(src) && ids.has(tgt);
    });

    // Shows with no surviving link stay on screen. They were being dropped,
    // which at a 10% overlap threshold silently removed 58 of 91 shows — and
    // "this show shares almost nobody" is a finding, not an absence of one.
    // With no edges holding them they drift to the edge of their own accord.
    return { nodes, links };
  }, [showGraph, currentFilters.minShared, currentFilters.minOverlap, currentFilters.weighting]);

  const showingShows = viewMode === 'shows';
  const showNameById = useMemo(
    () => new Map((showGraph?.nodes || []).map(n => [n.id, n.name])),
    [showGraph]
  );
  const activeGraphData = showingShows ? filteredShowData : filteredGraphData;

  // Interaction handlers read this rather than closing over one graph, so a
  // click in the shows view does not search the person links and find nothing.
  const activeLinksRef = useRef([]);
  activeLinksRef.current = activeGraphData.links;

  // Update visible stats when filtered data changes
  useEffect(() => {
    if (showingShows) {
      setNetworkStats(prev => ({
        ...prev,
        visiblePodcasts: filteredShowData.nodes.length,
        visibleLinks: filteredShowData.links.length,
      }));
      return;
    }
    const visiblePodcasts = new Set(filteredGraphData.links.map(l => l.podcast)).size;
    setNetworkStats(prev => ({
      ...prev,
      visibleNodes: filteredGraphData.nodes.length,
      visibleLinks: filteredGraphData.links.length,
      visiblePodcasts,
    }));
  }, [filteredGraphData, filteredShowData, showingShows]);

  // All podcasts for legend
  // The legend colours the links on screen, so it lists the shows those links
  // belong to. Taking them from the unfiltered data made it disagree with the
  // Podcasts stat — 78 against 73 — and name shows with nothing drawn for them.
  // In the shows view the nodes are the podcasts, so a colour key listing them
  // would restate the labels already on screen.
  const visiblePodcastList = useMemo(() =>
    showingShows ? [] : [...new Set(
      filteredGraphData.links
        .filter(l => l.value >= currentFilters.drawMinEpisodes)
        .map(l => l.podcast)
    )].sort(),
    [filteredGraphData, currentFilters.drawMinEpisodes, showingShows]
  );

  // Interaction handlers
  const clearSelection = useCallback(() => {
    setSelectedPodcast(null);
    setSelectedNode(null); setSelectedNodeConnections([]); setSelectedLink(null);
    setHighlightNodes(new Set()); setHighlightLinks(new Set()); setSelectedLinks(new Set());
  }, []);

  // Picking a show in the legend lights up its whole subgraph — every link
  // recorded on that show and the people at both ends. It reuses the same
  // highlight set a node click uses, so the two cannot both be lit at once.
  const handlePodcastSelect = useCallback(podcast => {
    setSelectedPodcast(podcast);
    setSelectedNode(null); setSelectedLink(null); setSelectedNodeConnections([]);
    if (!podcast) {
      setHighlightNodes(new Set()); setHighlightLinks(new Set()); setSelectedLinks(new Set());
      return;
    }
    const links = activeLinksRef.current.filter(l => l.podcast === podcast);
    const people = new Set();
    links.forEach(l => {
      people.add(l.source?.id ?? l.source);
      people.add(l.target?.id ?? l.target);
    });
    setSelectedLinks(new Set(links));
    setHighlightLinks(new Set(links));
    setHighlightNodes(people);
  }, []);

  // Switching view drops a selection that belongs to the other graph, and
  // re-arms the fit: 91 shows and 1,952 people occupy very different extents.
  useEffect(() => {
    clearSelection();
    hasAutoFitted.current = false;
  }, [viewMode, clearSelection]);

  // Keep the address bar in step. replaceState rather than pushState: toggling
  // the view a few times should not bury the page the visitor arrived from
  // under a stack of back presses.
  useEffect(() => {
    try {
      const url = new URL(window.location.href);
      if (viewMode === 'shows') url.searchParams.set('view', 'shows');
      else url.searchParams.delete('view');
      window.history.replaceState(null, '', url.toString());
    } catch {
      /* history is unavailable in some sandboxed frames; the view still works */
    }
  }, [viewMode]);

  const handleNodeClick = useCallback(node => {
    if (selectedNode?.id === node.id) {
      setSelectedNode(null); setSelectedNodeConnections([]);
      setHighlightNodes(new Set()); setHighlightLinks(new Set()); setSelectedLinks(new Set());
    } else {
      setSelectedNode(node); setSelectedLink(null);
      const conns = activeLinksRef.current.filter(
        l => l.source.id === node.id || l.target.id === node.id);
      setSelectedNodeConnections(conns);
      setHighlightNodes(new Set([node.id]));
      setSelectedLinks(new Set(conns));
      setHighlightLinks(new Set(conns));
      if (window.innerWidth < MOBILE_BREAKPOINT) setSidebarOpen(true);
    }
  }, [selectedNode]);

  const handleNodeHover = useCallback(node => {
    if (!node) {
      setHighlightNodes(selectedNode ? new Set([selectedNode.id]) : new Set());
      setHighlightLinks(selectedLinks);
      return;
    }
    const conns = activeLinksRef.current.filter(
      l => l.source.id === node.id || l.target.id === node.id);
    setHighlightNodes(new Set([node.id, ...(selectedNode ? [selectedNode.id] : [])]));
    setHighlightLinks(new Set([...selectedLinks, ...conns]));
  }, [selectedNode, selectedLinks]);

  const handleLinkClick = useCallback(link => {
    setSelectedLink(link); setSelectedNode(null);
    if (window.innerWidth < MOBILE_BREAKPOINT) setSidebarOpen(true);
  }, []);

  const handleFiltersChange = useCallback(partial => {
    setCurrentFilters(prev => ({ ...prev, ...partial }));
  }, []);

  // Node canvas painter — uses pre-loaded image cache
  // Use ref so the painter always has fresh values — useCallback with imageCache
  // (a ref) causes stale closure bugs where nodes render with wrong color/font
  const nodeCanvasObjectRef = useRef(null);
  nodeCanvasObjectRef.current = (node, ctx, globalScale) => {
    const size = nodeRadius(node);
    const isSelected = selectedNode?.id === node.id;
    const isHighlighted = highlightNodes.has(node.id);

    // Ring color
    const ringColor = isSelected ? '#dc2626'
      : isHighlighted ? '#f59e0b'
      : node.isShow ? podcastColor(node.name)
      : node.podcasts?.length > 0 ? podcastColor(node.podcasts[0]) : '#94a3b8';

    // Outer ring (podcast/state color) — 2px
    ctx.beginPath();
    ctx.arc(node.x, node.y, size + 1.5, 0, 2 * Math.PI);
    ctx.fillStyle = ringColor;
    ctx.fill();

    // Draw node circle — photo if available, initials if not
    ctx.save();
    ctx.beginPath();
    ctx.arc(node.x, node.y, size, 0, 2 * Math.PI);
    ctx.clip();

    const proxiedSrc = node.image && !node.image.includes('mzstatic.com')
      ? `${API_BASE_URL}/api/proxy/image?url=${encodeURIComponent(node.image)}`
      : node.image;
    const img = node.image ? (imageCache.current[proxiedSrc] || imageCache.current[node.image]) : null;
    if (img?.complete && img.naturalWidth > 0) {
      // Real Apple profile photo
      ctx.drawImage(img, node.x - size, node.y - size, size * 2, size * 2);
    } else {
      // Canvas-drawn initials — no external request, always consistent
      ctx.fillStyle = '#e5e7eb';
      ctx.fill();
      ctx.restore();
      ctx.save();
      ctx.fillStyle = '#1f2937';
      ctx.font = `bold ${Math.max(size * 0.85, 4)}px sans-serif`;
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      const initials = node.name.split(/[\s—–-]+/).map(w => w[0]).filter(Boolean).join('').slice(0, 2).toUpperCase();
      ctx.fillText(initials, node.x, node.y);
    }
    ctx.restore();

    // Shows carry no drawn label at all: the cover art identifies them, and 91
    // titles at once buried the artwork they were labelling. Hovering still
    // gives the full name through nodeLabel.
    // People keep the high-zoom label — initials are not an identity.
    if (!node.isShow && globalScale >= 2.5) {
      const fontSize = 10 / globalScale;
      const label = node.name;
      ctx.font = `${fontSize}px Arial`;
      const textWidth = ctx.measureText(label).width;
      const padding = 2 / globalScale;
      const labelY = node.y + size + 3 / globalScale;

      // Background pill
      ctx.fillStyle = 'rgba(255,255,255,0.85)';
      ctx.beginPath();
      ctx.roundRect(
        node.x - textWidth / 2 - padding,
        labelY - padding,
        textWidth + padding * 2,
        fontSize + padding * 2,
        2 / globalScale
      );
      ctx.fill();

      // Text
      ctx.fillStyle = '#1e293b';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'top';
      ctx.fillText(label, node.x, labelY);
    }
  };
  const nodeCanvasObject = useCallback((node, ctx, globalScale) => {
    nodeCanvasObjectRef.current?.(node, ctx, globalScale);
  }, []); // stable reference, always calls fresh painter via ref

  // Drawing-only threshold. force-graph filters linkVisibility in the paint and
  // hit-test passes but feeds the whole link array to the force, so hiding a
  // link declutters the view without moving a single node — unlike Minimum
  // Episodes Together, which removes them from the layout and relaxes it.
  const linkVisibility = useCallback(
    // Only meaningful for the person graph: there link.value is episodes
    // together, in the shows view it is people in common, and carrying a
    // threshold across would hide edges for reasons the slider does not say.
    link => showingShows || link.value >= currentFilters.drawMinEpisodes,
    [currentFilters.drawMinEpisodes, showingShows]
  );

  const nodePointerAreaPaint = useCallback((node, color, ctx) => {
    const size = nodeRadius(node);
    ctx.beginPath();
    ctx.arc(node.x, node.y, size + 4, 0, 2 * Math.PI);
    ctx.fillStyle = color;
    ctx.fill();
  }, []);

  return (
    <div className="flex h-screen w-full relative">
      {/* Mobile menu toggle */}
      <button
        onClick={() => setSidebarOpen(true)}
        className="md:hidden absolute top-4 left-4 z-30 bg-white rounded-lg shadow-lg p-2 text-gray-700"
        aria-label="Open menu"
      >
        ☰
      </button>

      {/* Mobile backdrop */}
      {sidebarOpen && (
        <div
          className="md:hidden fixed inset-0 bg-black/30 z-20"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* Sidebar */}
      <div
        className={`fixed md:static inset-y-0 left-0 w-96 max-w-[85vw] md:max-w-none md:min-w-[24rem] bg-gray-50 p-4 overflow-y-auto shadow-lg z-20 transform transition-transform duration-200 ease-in-out flex flex-col md:translate-x-0 ${
          sidebarOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
      >
        <div className="mb-3 flex items-start justify-between">
          <div>
            <h1 className="text-xl font-bold text-gray-800">Podcast Network</h1>
            <p className="text-gray-500 text-xs">Explore host connections</p>
          </div>
          <div className="flex items-center gap-2 flex-shrink-0">
            <a
              href="/stats"
              className="text-xs text-gray-400 hover:text-gray-600 border border-gray-200 rounded px-2 py-1 mt-1"
            >
              📊 Stats
            </a>
            <a
              href="/admin"
              className="text-xs text-gray-400 hover:text-gray-600 border border-gray-200 rounded px-2 py-1 mt-1"
            >
              ⚙ Admin
            </a>
            <button
              onClick={() => setSidebarOpen(false)}
              className="md:hidden text-gray-400 hover:text-gray-600 border border-gray-200 rounded px-2 py-1 mt-1"
              aria-label="Close menu"
            >
              ✕
            </button>
          </div>
        </div>

        {selectedNode && selectedNode.isShow ? (
          <ShowProfileCard
            show={selectedNode}
            connections={selectedNodeConnections}
            weighting={currentFilters.weighting}
            allLinks={showGraph?.links || []}
            nameById={showNameById}
            threshold={currentFilters.weighting === 'jaccard'
              ? currentFilters.minOverlap : currentFilters.minShared}
            onClose={clearSelection}
          />
        ) : selectedNode ? (
          <HostProfileCard
            host={selectedNode}
            connections={selectedNodeConnections}
            isAdmin={isAdmin}
            onClose={() => {
              setSelectedNode(null); setSelectedNodeConnections([]);
              setHighlightNodes(new Set()); setHighlightLinks(new Set()); setSelectedLinks(new Set());
            }}
          />
        ) : selectedLink ? (
          <ConnectionDetails
            connection={selectedLink}
            onClose={() => {
              setSelectedLink(null);
              setHighlightNodes(new Set()); setHighlightLinks(new Set()); setSelectedLinks(new Set());
            }}
          />
        ) : (
          <FilterPanel
            onFiltersChange={handleFiltersChange}
            networkStats={networkStats}
            currentFilters={currentFilters}
            searchQuery={searchQuery}
            onSearchChange={setSearchQuery}
            loading={loading}
            isAdmin={isAdmin}
            viewMode={viewMode}
            onViewModeChange={setViewMode}
            showGraphError={showGraphError}
          />
        )}

        {lastUpdated && (
          <p className="mt-auto pt-3 text-xs text-gray-400 text-center">
            Data updated {formatRelativeTime(lastUpdated)}
          </p>
        )}
      </div>

      {/* Graph */}
      <div className="flex-1 relative">
        {loading && (
          <div className="absolute inset-0 flex items-center justify-center bg-white">
            <div className="text-lg font-medium text-gray-500">Loading network data…</div>
          </div>
        )}

        {!loading && error && (
          <div className="absolute inset-0 flex flex-col items-center justify-center gap-4 text-center px-4 bg-white">
            <div className="text-xl font-semibold text-red-500">Couldn't load the network data</div>
            <div className="text-sm text-gray-500 max-w-md">{error}</div>
            <button
              className="px-4 py-2 bg-teal-600 text-white rounded hover:bg-teal-700"
              onClick={loadData}
            >
              Retry
            </button>
          </div>
        )}

        {!loading && !error && (
        <ForceGraph2D
          ref={installForces}
          graphData={activeGraphData}

          // Node rendering
          nodeRelSize={0}
          nodeCanvasObject={nodeCanvasObject}
          nodePointerAreaPaint={nodePointerAreaPaint}
          nodeLabel={node => node.name}

          // Link rendering
          linkLabel={link => link.isShowLink
            ? `${link.value} people in common · ${overlapPct(link.jaccard)}% of their combined guests`
            : `${link.value} episode${link.value !== 1 ? 's' : ''} on ${link.podcast}`}
          linkWidth={link => {
            if (link.isShowLink) {
              // Jaccard runs 0-0.3 in practice, raw runs 1-44; each gets its
              // own curve so the widths are comparable between weightings.
              const w = currentFilters.weighting === 'jaccard'
                ? Math.min(7, Math.max(0.6, link.jaccard * 26))
                : Math.min(7, Math.max(0.6, Math.sqrt(link.value) * 1.1));
              return (selectedLinks.has(link) || highlightLinks.has(link)) ? w + 2 : w;
            }
            // Was value/2 capped at 10, which saturated at 20 episodes: 36
            // links sat at the cap and a 20-episode tie was drawn the same as
            // a 350-episode one. Square root keeps separating them all the way
            // up, and takes 30% off the total link ink — the widest link is
            // now about the radius of the smallest node rather than twice it.
            const w = Math.min(6, Math.max(0.5, Math.sqrt(link.value) * 0.7));
            return (selectedLinks.has(link) || highlightLinks.has(link)) ? w + 2 : w;
          }}
          linkColor={link =>
            selectedLinks.has(link) || highlightLinks.has(link)
              ? '#f59e0b'
              : link.isShowLink
                ? `rgba(100, 116, 139, ${currentFilters.weighting === 'jaccard'
                    ? Math.min(0.65, 0.1 + link.jaccard * 2.2)
                    : Math.min(0.65, 0.1 + link.value / 55)})`
                : linkColor(link.podcast, Math.min(0.6, 0.12 + Math.sqrt(link.value) * 0.1))
          }
          linkDirectionalParticles={link =>
            selectedLinks.has(link) || highlightLinks.has(link) ? 3 : 0
          }
          linkDirectionalParticleWidth={2}
          linkCurvature={link => link.curvature || 0}
          linkVisibility={linkVisibility}
          // Links are thin and crowded; the 4px default makes them fiddly to hit.
          linkHoverPrecision={8}

          // Forces
          // No linkDistance / linkStrength / d3ForceStrength here: force-graph
          // has never had those props — it builds forceLink() and
          // forceManyBody() with no arguments, so the layout has always run on
          // d3's defaults (link distance 30, charge -30) and the three values
          // set here were read by nothing. To really change them, reach the
          // force through d3Force('link') / d3Force('charge') as installForces
          // does above, and re-measure: they move the layout a long way.
          // Was 0.01, which needs ~690 ticks to converge. d3's own default gets
          // there in 300 with an identical result — measured on the live graph,
          // same zero overlaps, same zero crowding.
          d3AlphaDecay={0.0228}
          d3VelocityDecay={0.4}
          // The engine stopped only on cooldownTime, so it churned for the full
          // 15s default no matter how settled the layout was — the drifting and
          // wiggling. alphaMin lets it stop when it converges instead; nodes are
          // moving 0.014u/tick by then, so there is no visible snap.
          d3AlphaMin={0.001}
          cooldownTicks={Infinity}

          // Dimensions & interaction
          width={dimensions.width}
          height={dimensions.height}
          enableZoomInteraction
          enablePanInteraction
          minZoom={0.05}
          maxZoom={4}
          onEngineStop={() => {
            // The graph was never fitted to the canvas, which is where the wide
            // empty margin came from. Only on the first settle — refitting on
            // every filter change would yank the view out from under anyone who
            // had zoomed in.
            if (hasAutoFitted.current) return;
            hasAutoFitted.current = true;
            graphRef.current?.zoomToFit(400, 30);
          }}

          // Events
          onBackgroundClick={clearSelection}
          onNodeClick={handleNodeClick}
          onNodeHover={handleNodeHover}
          onLinkClick={handleLinkClick}
        />
        )}
      </div>

      {/* Legend — a colour key for podcasts, which in the shows view are the
          nodes themselves, labelled by their own artwork. */}
      {!loading && !error && !showingShows && (
        <PodcastLegend
          podcasts={visiblePodcastList}
          isOpen={legendOpen}
          onToggle={() => setLegendOpen(o => !o)}
          selected={selectedPodcast}
          onSelect={handlePodcastSelect}
        />
      )}
    </div>
  );
};

export default PodcastHostNetwork;
