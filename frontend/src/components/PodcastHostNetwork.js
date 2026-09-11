import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { ForceGraph2D } from 'react-force-graph';
import { forceX, forceY } from 'd3-force';
import { API_BASE_URL } from '../config';

// ─── Constants ────────────────────────────────────────────────────────────────

const API_URL = `${API_BASE_URL}/api/host-connections`;
const SIDEBAR_WIDTH = 384;
const MOBILE_BREAKPOINT = 768;

// Below the mobile breakpoint the sidebar overlays the graph instead of
// pushing it over, so the graph should use the full viewport width.
const getGraphWidth = () =>
  window.innerWidth >= MOBILE_BREAKPOINT ? window.innerWidth - SIDEBAR_WIDTH : window.innerWidth;

// ─── Helpers ──────────────────────────────────────────────────────────────────

const getAvatarUrl = (name) =>
  `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(name)}&backgroundColor=65c9ff,92a1c6,dd6b7f,58c9b9,ade498`;

// Stable color per podcast title (deterministic, no random)
const podcastColor = (title) => {
  let hash = 0;
  for (let i = 0; i < title.length; i++) {
    hash = title.charCodeAt(i) + ((hash << 5) - hash);
  }
  return `hsla(${Math.abs(hash) % 360}, 65%, 55%, 0.85)`;
};

// Process raw API data into graph nodes/links — defined OUTSIDE component
const processData = (data) => {
  const nodes = new Map();
  const links = [];

  data.forEach(conn => {
    // Source node
    if (!nodes.has(conn.source_id)) {
      nodes.set(conn.source_id, {
        id: conn.source_id,
        name: conn.source_name,
        image: conn.source_image || null,
        role: conn.source_role || 'Host',
        channel: conn.source_channel,
        genre: conn.source_genre,
        val: 0,
        podcasts: new Set(),
      });
    }
    nodes.get(conn.source_id).podcasts.add(conn.podcast_title);
    nodes.get(conn.source_id).val++;

    // Target node
    if (!nodes.has(conn.target_id)) {
      nodes.set(conn.target_id, {
        id: conn.target_id,
        name: conn.target_name,
        image: conn.target_image || null,
        role: conn.target_role || 'Host',
        channel: conn.target_channel,
        genre: conn.target_genre,
        val: 0,
        podcasts: new Set(),
      });
    }
    nodes.get(conn.target_id).podcasts.add(conn.podcast_title);
    nodes.get(conn.target_id).val++;

    links.push({
      source: conn.source_id,
      target: conn.target_id,
      value: conn.episodes_together,
      podcast: conn.podcast_title,
    });
  });

  // Convert Sets to Arrays
  nodes.forEach(node => { node.podcasts = Array.from(node.podcasts); });

  return { nodes: Array.from(nodes.values()), links };
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

const HostProfileCard = ({ host, connections, onClose }) => {
  const uniquePodcasts = new Set(connections.map(c => c.podcast)).size;
  const totalEpisodes = connections.reduce((s, c) => s + c.value, 0);

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
        <h3 className="text-xl font-bold text-center">{host.name}</h3>
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
        <h4 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-2">Top Co-Hosts</h4>
        <div className="space-y-2">
          {connections
            .sort((a, b) => b.value - a.value)
            .slice(0, 5)
            .map((conn, idx) => (
              <div key={idx} className="bg-gray-50 p-2 rounded text-sm">
                <p className="font-medium">
                  {conn.target.name === host.name ? conn.source.name : conn.target.name}
                </p>
                <p className="text-gray-500 text-xs">
                  {conn.value} episode{conn.value !== 1 ? 's' : ''} together on {conn.podcast}
                </p>
              </div>
            ))}
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
        <h4 className="text-sm font-semibold text-gray-500 uppercase tracking-wide mb-2">Hosts</h4>
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

const FilterPanel = ({ onFiltersChange, networkStats, currentFilters, searchQuery, onSearchChange }) => (
  <div className="space-y-4">
    <div className="p-2.5 bg-teal-50 rounded-lg text-sm text-gray-600">
      Showing <span className="font-bold text-gray-900">{networkStats.visibleNodes}</span> hosts with{' '}
      <span className="font-bold text-gray-900">{networkStats.visibleLinks}</span> connections from{' '}
      <span className="font-bold text-gray-900">{networkStats.visiblePodcasts}</span> podcasts
    </div>

    {/* Name search */}
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

    {/* Min cluster size */}
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

    {/* Genre & Channel */}
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

    <button
      onClick={() => onFiltersChange({
        minConnections: 1, minPodcasts: 1,
        minClusterSize: 8,
        selectedRoles: ['Host', 'Guest'],
        selectedChannel: 'all', selectedGenre: 'all',
      })}
      className="w-full py-2 px-4 bg-gray-100 hover:bg-gray-200 rounded-md text-sm font-medium text-gray-600"
    >
      Reset Filters
    </button>
  </div>
);

const PodcastLegend = ({ podcasts, isOpen, onToggle }) => (
  <div className="absolute bottom-4 right-4 bg-white rounded-lg shadow-lg max-w-xs">
    <button
      onClick={onToggle}
      className="w-full flex items-center justify-between px-4 py-2 text-sm font-semibold text-gray-700"
    >
      <span>Podcast Clusters ({podcasts.length})</span>
      <span>{isOpen ? '▲' : '▼'}</span>
    </button>
    {isOpen && (
      <div className="px-4 pb-3 space-y-1 max-h-64 overflow-y-auto">
        {podcasts.map(p => (
          <div key={p} className="flex items-center text-xs">
            <div className="w-3 h-3 rounded-full mr-2 flex-shrink-0" style={{ backgroundColor: podcastColor(p) }} />
            <span className="truncate">{p}</span>
          </div>
        ))}
      </div>
    )}
  </div>
);

// ─── Main component ───────────────────────────────────────────────────────────

const PodcastHostNetwork = () => {
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [highlightNodes, setHighlightNodes] = useState(new Set());
  const [highlightLinks, setHighlightLinks] = useState(new Set());
  const [selectedNode, setSelectedNode] = useState(null);
  const [selectedLink, setSelectedLink] = useState(null);
  const [selectedNodeConnections, setSelectedNodeConnections] = useState([]);
  const [selectedLinks, setSelectedLinks] = useState(new Set());
  const [legendOpen, setLegendOpen] = useState(true);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [dimensions, setDimensions] = useState({
    width: getGraphWidth(),
    height: window.innerHeight,
  });
  const [currentFilters, setCurrentFilters] = useState({
    minConnections: 1,
    minPodcasts: 1,
    minClusterSize: 8,
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
  const imageCache = useImageCache(graphData.nodes);

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
      .then(data => setGraphData(processData(Array.isArray(data) ? data : [])))
      .catch(err => setError(err.message || 'Failed to load network data'))
      .finally(() => setLoading(false));
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);



  // Compute network stats when base data loads
  useEffect(() => {
    if (!graphData.nodes.length) return;
    const maxConnections = Math.max(...graphData.nodes.map(n => n.val));
    const maxPodcasts = Math.max(...graphData.nodes.map(n => n.podcasts.length));
    const channels = [...new Set(graphData.nodes.map(n => n.channel).filter(Boolean))].sort();
    const genres = [...new Set(graphData.nodes.map(n => n.genre).filter(Boolean))].sort();
    setNetworkStats(prev => ({ ...prev, maxConnections, maxPodcasts, channels, genres }));
  }, [graphData]);

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
      const src = l.source?.id ?? l.source;
      const tgt = l.target?.id ?? l.target;
      return validIds.has(src) && validIds.has(tgt);
    });

    return filterSmallClusters({ nodes: filteredNodes, links: filteredLinks }, currentFilters.minClusterSize);
  }, [graphData, currentFilters, searchQuery]);

  // Update visible stats when filtered data changes
  useEffect(() => {
    const visiblePodcasts = new Set(filteredGraphData.links.map(l => l.podcast)).size;
    setNetworkStats(prev => ({
      ...prev,
      visibleNodes: filteredGraphData.nodes.length,
      visibleLinks: filteredGraphData.links.length,
      visiblePodcasts,
    }));
  }, [filteredGraphData]);

  // All podcasts for legend
  const allPodcasts = useMemo(() =>
    [...new Set(graphData.nodes.flatMap(n => n.podcasts))].sort(),
    [graphData]
  );

  // Interaction handlers
  const handleNodeClick = useCallback(node => {
    if (selectedNode?.id === node.id) {
      setSelectedNode(null); setSelectedNodeConnections([]);
      setHighlightNodes(new Set()); setHighlightLinks(new Set()); setSelectedLinks(new Set());
    } else {
      setSelectedNode(node); setSelectedLink(null);
      const conns = graphData.links.filter(l => l.source.id === node.id || l.target.id === node.id);
      setSelectedNodeConnections(conns);
      setHighlightNodes(new Set([node.id]));
      setSelectedLinks(new Set(conns));
      setHighlightLinks(new Set(conns));
      if (window.innerWidth < MOBILE_BREAKPOINT) setSidebarOpen(true);
    }
  }, [selectedNode, graphData.links]);

  const handleNodeHover = useCallback(node => {
    if (!node) {
      setHighlightNodes(selectedNode ? new Set([selectedNode.id]) : new Set());
      setHighlightLinks(selectedLinks);
      return;
    }
    const conns = graphData.links.filter(l => l.source.id === node.id || l.target.id === node.id);
    setHighlightNodes(new Set([node.id, ...(selectedNode ? [selectedNode.id] : [])]));
    setHighlightLinks(new Set([...selectedLinks, ...conns]));
  }, [selectedNode, selectedLinks, graphData.links]);

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
    const size = node.val <= 1 ? 3.5 : 4 + Math.sqrt(node.val) * 2.2;
    const isSelected = selectedNode?.id === node.id;
    const isHighlighted = highlightNodes.has(node.id);

    // Ring color
    const ringColor = isSelected ? '#dc2626'
      : isHighlighted ? '#f59e0b'
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
      const initials = node.name.split(' ').map(w => w[0]).filter(Boolean).join('').slice(0, 2).toUpperCase();
      ctx.fillText(initials, node.x, node.y);
    }
    ctx.restore();

    // Label only at high zoom — small, with background pill for readability
    if (globalScale >= 2.5) {
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

  const nodePointerAreaPaint = useCallback((node, color, ctx) => {
    const size = node.val <= 1 ? 3.5 : 4 + Math.sqrt(node.val) * 2.2;
    ctx.beginPath();
    ctx.arc(node.x, node.y, size + 4, 0, 2 * Math.PI);
    ctx.fillStyle = color;
    ctx.fill();
  }, []);

  if (loading) return (
    <div className="flex items-center justify-center h-screen">
      <div className="text-xl font-semibold text-gray-600">Loading network data...</div>
    </div>
  );

  if (error) return (
    <div className="flex flex-col items-center justify-center h-screen gap-4 text-center px-4">
      <div className="text-xl font-semibold text-red-500">Couldn't load the network data</div>
      <div className="text-sm text-gray-500 max-w-md">{error}</div>
      <button
        className="px-4 py-2 bg-teal-600 text-white rounded hover:bg-teal-700"
        onClick={loadData}
      >
        Retry
      </button>
    </div>
  );

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
        className={`fixed md:static inset-y-0 left-0 w-96 max-w-[85vw] md:max-w-none md:min-w-[24rem] bg-gray-50 p-4 overflow-y-auto shadow-lg z-20 transform transition-transform duration-200 ease-in-out md:translate-x-0 ${
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

        {selectedNode ? (
          <HostProfileCard
            host={selectedNode}
            connections={selectedNodeConnections}
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
          />
        )}
      </div>

      {/* Graph */}
      <div className="flex-1 relative">
        <ForceGraph2D
          ref={graphRef}
          graphData={filteredGraphData}

          // Node rendering
          nodeRelSize={0}
          nodeCanvasObject={nodeCanvasObject}
          nodePointerAreaPaint={nodePointerAreaPaint}
          nodeLabel={node => node.name}

          // Link rendering
          linkLabel={link => `${link.value} episode${link.value !== 1 ? 's' : ''} on ${link.podcast}`}
          linkWidth={link => {
            const w = Math.min(10, Math.max(1, link.value / 2));
            return (selectedLinks.has(link) || highlightLinks.has(link)) ? w + 2 : w;
          }}
          linkColor={link =>
            selectedLinks.has(link) || highlightLinks.has(link)
              ? '#f59e0b'
              : `rgba(156, 163, 175, ${Math.min(0.8, link.value / 5)})`
          }
          linkDirectionalParticles={link =>
            selectedLinks.has(link) || highlightLinks.has(link) ? 3 : 0
          }
          linkDirectionalParticleWidth={2}

          // Forces
          d3ForceStrength={-180}
          d3AlphaDecay={0.01}
          d3VelocityDecay={0.4}
          linkDistance={70}
          linkStrength={0.2}
          cooldownTicks={Infinity}

          // Dimensions & interaction
          width={dimensions.width}
          height={dimensions.height}
          enableZoomInteraction
          enablePanInteraction
          minZoom={0.05}
          maxZoom={4}
          onEngineTick={() => {
            // Set centering forces on first tick — onEngineStart doesn't exist in v1.46
            if (!graphRef.current) return;
            if (!graphRef.current._forcesSet) {
              graphRef.current.d3Force('x', forceX(0).strength(0.08));
              graphRef.current.d3Force('y', forceY(0).strength(0.08));
              graphRef.current._forcesSet = true;
            }
          }}

          // Events
          onNodeClick={handleNodeClick}
          onNodeHover={handleNodeHover}
          onLinkClick={handleLinkClick}
        />
      </div>

      {/* Legend */}
      <PodcastLegend
        podcasts={allPodcasts}
        isOpen={legendOpen}
        onToggle={() => setLegendOpen(o => !o)}
      />
    </div>
  );
};

export default PodcastHostNetwork;
