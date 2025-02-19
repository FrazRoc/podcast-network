import React, { useState, useEffect } from 'react';
import { ForceGraph2D } from 'react-force-graph';
import { 
  forceSimulation,
  forceLink,
  forceManyBody,
  forceCenter,
  forceCollide
} from 'd3-force';

// Add this helper function at the top of your file
const getAvatarUrl = (name) => {
  // Use DiceBear's avataaars collection
  return `https://api.dicebear.com/7.x/initials/svg?seed=${encodeURIComponent(name)}&backgroundColor=65c9ff,92a1c6,dd6b7f,58c9b9,ade498`;
};

const HostProfileCard = ({ host, connections, onClose }) => {
  const borderColor = "rgb(148, 163, 184)"; // #94a3b8 in rgb for consistency
  const uniquePodcasts = new Set(connections.map(conn => conn.podcast)).size;


  return (
    <div className="bg-white rounded-lg shadow-lg p-6 relative">
      <CloseButton onClick={onClose} />
      {/* Center profile section */}
      <div className="flex flex-col items-center mb-6 ">
        {/* Profile image with border - using double div for border effect */}
        <div className="w-24 h-24 rounded-full p-0.5 mb-3"
             style={{ backgroundColor: borderColor }}>
          <div className="w-full h-full rounded-full overflow-hidden bg-white">
            <img 
              src={host.image || getAvatarUrl(host.name)} 
              alt={host.name}
              className="w-full h-full object-cover"
              onError={(e) => {
                e.target.onerror = null;
                e.target.src = getAvatarUrl(host.name);
              }}
            />
          </div>
        </div>
        {/* Name and connections count EF: Do we want this? it seems to break with new filtering feature*/}
        <h3 className="text-xl font-bold text-center">{host.name}</h3>
        <p className="text-gray-600 text-sm">{connections.length} connections</p>
      </div>

      <div className="mb-4">
        <h4 className="text-lg font-semibold mb-2">Stats</h4>
        <div className="grid grid-cols-3 gap-2 text-sm">
          <div className="bg-blue-50 p-2 rounded">
            <p className="text-gray-600">Podcasts</p>
            <p className="font-bold">{uniquePodcasts}</p>
          </div>
          <div className="bg-blue-50 p-2 rounded">
            <p className="text-gray-600">Episodes</p>
            <p className="font-bold">{connections.reduce((sum, c) => sum + c.value, 0)}</p>
          </div>
          <div className="bg-blue-50 p-2 rounded">
            <p className="text-gray-600">Connections</p>
            <p className="font-bold">{host.val}</p>
          </div>
        </div>
      </div>

      <div>
        <h4 className="text-lg font-semibold mb-2">Top Co-Hosts</h4>
        <div className="space-y-2">
          {connections
            .sort((a, b) => b.value - a.value)
            .slice(0, 5)
            .map((conn, idx) => (
              <div key={idx} className="bg-gray-50 p-2 rounded text-sm">
                <p className="font-medium">
                  {conn.target.name === host.name ? conn.source.name : conn.target.name}
                </p>
                <p className="text-gray-600 text-xs">
                  {conn.value} episodes together on {conn.podcast}
                </p>
              </div>
            ))}
        </div>
      </div>
    </div>
  );
};

const ConnectionDetails = ({ connection, onClose }) => {
  return (
    <div className="bg-white rounded-lg shadow-lg p-6 relative" >
      <CloseButton onClick={onClose} />
      <h3 className="text-xl font-bold mb-4">Connection Details</h3>
      
      <div className="space-y-4">
        <div>
          <h4 className="text-lg font-semibold mb-2">Hosts</h4>
          <div className="bg-blue-50 p-3 rounded space-y-2">
            <p className="font-medium">{connection.source.name}</p>
            <div className="flex items-center">
              <div className="flex-1 border-t border-gray-300"></div>
              <div className="px-2 text-gray-500 text-sm">with</div>
              <div className="flex-1 border-t border-gray-300"></div>
            </div>
            <p className="font-medium">{connection.target.name}</p>
          </div>
        </div>

        <div>
          <h4 className="text-lg font-semibold mb-2">Collaboration</h4>
          <div className="bg-blue-50 p-3 rounded">
            <p className="text-sm">
              <span className="font-bold">{connection.value}</span> episodes together
            </p>
            <p className="text-sm text-gray-600 mt-1">
              on <span className="font-medium">{connection.podcast}</span>
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

const PodcastLegend = ({ podcasts }) => {
  return (
    <div className="absolute bottom-4 right-4 bg-white p-4 rounded-lg shadow-lg max-w-xs">
      <h3 className="text-sm font-semibold mb-2">Podcast Clusters</h3>
      <div className="space-y-1">
        {podcasts.map((podcast, i) => (
          <div key={podcast} className="flex items-center text-xs">
            <div 
              className="w-3 h-3 rounded-full mr-2" 
              style={{ 
                backgroundColor: `hsla(${podcast.length * 7}, 70%, 70%, 0.6)`
              }} 
            />
            <span className="truncate">{podcast}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

// Add this close button component
const CloseButton = ({ onClick }) => (
  <button
    onClick={onClick}
    className="absolute top-2 right-2 w-8 h-8 flex items-center justify-center rounded-full hover:bg-gray-100"
  >
    <svg 
      xmlns="http://www.w3.org/2000/svg" 
      className="h-5 w-5 text-gray-500 hover:text-gray-700" 
      fill="none" 
      viewBox="0 0 24 24" 
      stroke="currentColor"
    >
      <path 
        strokeLinecap="round" 
        strokeLinejoin="round" 
        strokeWidth={2} 
        d="M6 18L18 6M6 6l12 12" 
      />
    </svg>
  </button>
);

const FilterPanel = ({ onFiltersChange, networkStats, currentFilters }) => {
  return (
    <div className="space-y-6">
      <h2 className="text-xl font-bold text-gray-800">Filter Network</h2>
      
      <div className="mt-2 p-3 bg-blue-50 rounded-lg">
          <div className="text-sm text-gray-600">
            Showing <span className="font-bold text-gray-900">{networkStats.visibleNodes}</span> hosts 
            with <span className="font-bold text-gray-900">{networkStats.visibleLinks}</span> connections
            from <span className="font-bold text-gray-900">{networkStats.visiblePodcasts}</span> podcasts
          </div>
      </div>

      {/* Connections Slider */}
      <div className="space-y-2">
        <label className="block text-sm font-medium text-gray-700">
          Minimum Connections: {currentFilters.minConnections}
        </label>
        <input
          type="range"
          min="2"
          max={Math.max(1, networkStats.maxConnections)} // Ensure max is at least 1
          value={currentFilters.minConnections}
          onChange={(e) => onFiltersChange({ minConnections: parseInt(e.target.value) })}
          className="w-full"
        />
        <div className="flex justify-between text-xs text-gray-500">
          <span>1</span>
          <span>{networkStats.maxConnections}</span>
        </div>
      </div>

      {/* Podcasts Slider */}
      <div className="space-y-2">
        <label className="block text-sm font-medium text-gray-700">
          Minimum Podcasts: {currentFilters.minPodcasts}
        </label>
        <input
          type="range"
          min="1"
          max={Math.max(1, networkStats.maxPodcasts)} // Ensure max is at least 1
          value={currentFilters.minPodcasts}
          onChange={(e) => onFiltersChange({ minPodcasts: parseInt(e.target.value) })}
          className="w-full"
        />
        <div className="flex justify-between text-xs text-gray-500">
          <span>1</span>
          <span>{networkStats.maxPodcasts}</span>
        </div>
      </div>

      {/* Role Checkboxes */}
      <div className="space-y-2">
        <label className="block text-sm font-medium text-gray-700">Roles</label>
        <div className="space-y-1">
          {['Host', 'Guest'].map(role => (
            <label key={`role-${role}`} className="flex items-center space-x-2">
              <input
                type="checkbox"
                checked={currentFilters.selectedRoles.includes(role)}
                onChange={(e) => {
                  const newRoles = e.target.checked
                    ? [...currentFilters.selectedRoles, role]
                    : currentFilters.selectedRoles.filter(r => r !== role);
                  onFiltersChange({ selectedRoles: newRoles });
                }}
                className="rounded text-blue-600"
              />
              <span className="text-sm text-gray-700">{role}</span>
            </label>
          ))}
        </div>
      </div>

      {/* Genre Dropdown */}
      <div className="space-y-2">
        <label className="block text-sm font-medium text-gray-700">Genre</label>
        <select
          value={currentFilters.selectedGenre}
          onChange={(e) => onFiltersChange({ selectedGenre: e.target.value })}
          className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
        >
          <option key="all" value="all">All Genres</option>
          {networkStats.genres?.map(genre => (
            <option key={`genre-${genre}`} value={genre}>
              {genre}
            </option>
          ))}
        </select>
      </div>

      {/* Channel Dropdown */}
      <div className="space-y-2">
        <label className="block text-sm font-medium text-gray-700">Channel</label>
        <select
          value={currentFilters.selectedChannel}
          onChange={(e) => onFiltersChange({ selectedChannel: e.target.value })}
          className="w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
        >
          <option key="all" value="all">All Channels</option>
          {networkStats.channels?.map(channel => (
            <option key={`channel-${channel}`} value={channel}>
              {channel}
            </option>
          ))}
        </select>
      </div>

      {/* Reset Button */}
      <button
        onClick={() => onFiltersChange({
          minConnections: 2,
          minPodcasts: 1,
          selectedRoles: ['Host', 'Guest'],
          selectedChannel: 'all',
          selectedGenre: 'all'
        })}
        className="w-full py-2 px-4 bg-gray-100 hover:bg-gray-200 rounded-md text-sm font-medium text-gray-600"
      >
        Reset Filters
      </button>
    </div>
  );
};

const PodcastHostNetwork = () => {
  // State declarations
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [highlightNodes, setHighlightNodes] = useState(new Set());
  const [highlightLinks, setHighlightLinks] = useState(new Set());
  const [selectedNode, setSelectedNode] = useState(null);
  const [selectedLink, setSelectedLink] = useState(null);
  const [selectedNodeConnections, setSelectedNodeConnections] = useState([]);
  const [selectedLinks, setSelectedLinks] = useState(new Set());
  const [dimensions, setDimensions] = useState({
    width: window.innerWidth - 384, // 384px is sidebar width (24rem)
    height: window.innerHeight
  });
  // Add state for filtered data
  const [filteredGraphData, setFilteredGraphData] = useState({ nodes: [], links: [] });
  const [networkStats, setNetworkStats] = useState({
    maxConnections: 0,
    maxPodcasts: 0,
    channels: [],
    genres: [],
    visibleNodes: 0,
    visibleLinks: 0
  });

  const [currentFilters, setCurrentFilters] = useState({
    minConnections: 2,
    minPodcasts: 1,
    selectedRoles: ['Host', 'Guest'],
    selectedChannel: 'all',
    selectedGenre: 'all'
  });

  // Add new state for loading
  const [isFilteringGraph, setIsFilteringGraph] = useState(false);

  // Initialize filteredGraphData with graphData when it first loads
  useEffect(() => {
    setFilteredGraphData(graphData);
  }, [graphData]);

  // Add a useEffect to handle window resizing
  useEffect(() => {
    const handleResize = () => {
      setDimensions({
        width: window.innerWidth - 384,
        height: window.innerHeight
      });
    };

    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  // Add a useEffect to update the visible counts when filteredGraphData changes
  useEffect(() => {
    // Get unique podcasts from visible links
    const uniquePodcasts = new Set(filteredGraphData.links.map(link => link.podcast));

    setNetworkStats(prev => ({
      ...prev,
      visibleNodes: filteredGraphData.nodes.length,
      visibleLinks: filteredGraphData.links.length,
      visiblePodcasts: uniquePodcasts.size
    }));
  }, [filteredGraphData]);

  // Update handleNodeClick to maintain selected links
  const handleNodeClick = node => {
    if (selectedNode && node.id === selectedNode.id) {
      // Clicking the selected node again deselects it
      setSelectedNode(null);
      setSelectedNodeConnections([]);
      setHighlightNodes(new Set());
      setHighlightLinks(new Set());
      setSelectedLinks(new Set());
    } else {
      // Select new node
      setSelectedNode(node);
      setSelectedLink(null);
      const nodeConnections = graphData.links.filter(
        link => link.source.id === node.id || link.target.id === node.id
      );
      setSelectedNodeConnections(nodeConnections);
      // Highlight the selected node and its connections
      setHighlightNodes(new Set([node.id]));
      setSelectedLinks(new Set(nodeConnections));
      setHighlightLinks(new Set(nodeConnections));
    }
  };

  // Update handleNodeHover to work with selected links
  const handleNodeHover = node => {
    if (!node) {
      setHighlightNodes(selectedNode ? new Set([selectedNode.id]) : new Set());
      setHighlightLinks(selectedLinks);
      return;
    }

    const connectedLinks = graphData.links.filter(
      link => link.source.id === node.id || link.target.id === node.id
    );

    setHighlightNodes(new Set([node.id, ...(selectedNode ? [selectedNode.id] : [])]));
    setHighlightLinks(new Set([...selectedLinks, ...connectedLinks]));
  };

  const handleLinkClick = link => {
    setSelectedLink(link);
    setSelectedNode(null);
  };

  // useEffect for data fetching
  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch('http://localhost:8000/api/host-connections');
        if (!response.ok) {
          throw new Error(`API call failed: ${response.status}`);
        }
        const data = await response.json();
        
        // Use the processData function here instead of inline processing
        setGraphData(processData(data));
        
      } catch (err) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  // Calculate network stats when data is loaded
  useEffect(() => {
    if (graphData.nodes.length) {
      
      // Calculate unique podcasts per node
      const nodePodcasts = new Map();
      graphData.links.forEach(link => {
        if (!nodePodcasts.has(link.source.id)) {
          nodePodcasts.set(link.source.id, new Set());
        }
        if (!nodePodcasts.has(link.target.id)) {
          nodePodcasts.set(link.target.id, new Set());
        }
        nodePodcasts.get(link.source.id).add(link.podcast);
        nodePodcasts.get(link.target.id).add(link.podcast);
      });

      // Find node with most connections
      const maxConnections = Math.max(...graphData.nodes.map(node => node.val));
      
      // Find node with most podcasts using the podcasts array we store on each node
      const maxPodcasts = Math.max(...graphData.nodes.map(node => node.podcasts.length));
      
      // Get unique channels
      const channels = Array.from(new Set(graphData.nodes.map(node => node.channel))).sort();
      
      const genres = Array.from(new Set(graphData.nodes.map(node => node.genre))).sort();


      setNetworkStats({
        maxConnections,
        maxPodcasts,
        channels,
        genres,
        visibleNodes: graphData.nodes.length,
        visibleLinks: graphData.links.length
      });
    }
  }, [graphData]);

  const handleFiltersChange = (filters) => {
    setIsFilteringGraph(true);
    
    setTimeout(() => {
      try {
        console.log('Starting filtering with:', filters);
        console.log('Original data:', graphData.nodes.length, 'nodes,', graphData.links.length, 'links');

        // First identify which nodes pass the filters
        const filteredNodes = graphData.nodes.filter(node => {
          console.log('Checking node:', node);
          
          // Check minimum connections
          if (node.val < filters.minConnections) {
            console.log('Failed connections check:', node.val, '<', filters.minConnections);
            return false;
          }

          // Calculate unique podcasts for this node
          const nodeLinks = graphData.links.filter(link => 
            (link.source.id || link.source) === node.id || 
            (link.target.id || link.target) === node.id
          );
          const nodePodcastCount = new Set(nodeLinks.map(link => link.podcast)).size;
          
          console.log('Podcast count for node:', nodePodcastCount);

          if (nodePodcastCount < filters.minPodcasts) {
            console.log('Failed podcast count check:', nodePodcastCount, '<', filters.minPodcasts);
            return false;
          }

           // Check role - with defensive programming
          if (filters.selectedRoles && filters.selectedRoles.length > 0) {
            const nodeRole = node.role || 'Host';  // Default to 'Host' if no role specified
            if (!filters.selectedRoles.includes(nodeRole)) {
              console.log('Failed role check:', nodeRole, 'not in', filters.selectedRoles);
              return false;
            }
          }

          // Check genre - with defensive programming
          if (filters.selectedGenre && filters.selectedGenre !== 'all') {
            const nodeGenre = node.genre;
            if (!nodeGenre || nodeGenre !== filters.selectedGenre) {
              console.log('Failed genre check:', nodeGenre, '!==', filters.selectedGenre);
              return false;
            }
          }

          // Check channel - with defensive programming
          if (filters.selectedChannel && filters.selectedChannel !== 'all') {
            const nodeChannel = node.channel;
            if (!nodeChannel || nodeChannel !== filters.selectedChannel) {
              console.log('Failed channel check:', nodeChannel, '!==', filters.selectedChannel);
              return false;
            }
          }

          return true;
        });

        console.log('Nodes after filtering:', filteredNodes);

        const validNodeIds = new Set(filteredNodes.map(node => node.id));
        
        const filteredLinks = graphData.links.filter(link => {
          const sourceId = link.source.id || link.source;
          const targetId = link.target.id || link.target;
          return validNodeIds.has(sourceId) && validNodeIds.has(targetId);
        });

        // Set the filtered data
        setFilteredGraphData({
          nodes: filteredNodes,
          links: filteredLinks
        });

        console.log(`Final filtered data: ${filteredNodes.length} nodes and ${filteredLinks.length} links`);
      } catch (error) {
        console.error('Error applying filters:', error);
      } finally {
        setIsFilteringGraph(false);
      }
    }, 100);
  };


// Add loading overlay component
const LoadingOverlay = () => (
  <div className="absolute inset-0 bg-white/50 flex items-center justify-center z-20">
    <div className="bg-white p-4 rounded-lg shadow-lg flex items-center space-x-3">
      <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-500"></div>
      <span className="text-gray-700">Updating graph...</span>
    </div>
  </div>
);


const processData = (data) => {
  const nodes = new Map();
  const links = [];
  const hostPodcasts = new Map(); // Track podcasts for each host
  
  // Process connections to build nodes and track podcasts
  data.forEach(connection => {
    // Track podcasts for source host
    if (!hostPodcasts.has(connection.source_id)) {
      hostPodcasts.set(connection.source_id, new Set());
    }
    hostPodcasts.get(connection.source_id).add(connection.podcast_title);

    // Track podcasts for target host
    if (!hostPodcasts.has(connection.target_id)) {
      hostPodcasts.set(connection.target_id, new Set());
    }
    hostPodcasts.get(connection.target_id).add(connection.podcast_title);

    // Create nodes if they don't exist
    if (!nodes.has(connection.source_id)) {
      nodes.set(connection.source_id, {
        id: connection.source_id,
        name: connection.source_name,
        image: connection.source_image,
        role: connection.source_role || 'Host',  // Default to 'Host' if null
        channel: connection.source_channel,
        genre: connection.source_genre,  // Add genre
        val: 1,
        podcasts: new Set([connection.podcast_title])
      });
    } else {
      nodes.get(connection.source_id).podcasts.add(connection.podcast_title);
    }
    
    if (!nodes.has(connection.target_id)) {
      nodes.set(connection.target_id, {
        id: connection.target_id,
        name: connection.target_name,
        image: connection.target_image,
        role: connection.target_role || 'Host',  // Default to 'Host' if null
        channel: connection.target_channel,
        genre: connection.target_genre,  // Add genre
        val: 1,
        podcasts: new Set([connection.podcast_title])
      });
    } else {
      nodes.get(connection.target_id).podcasts.add(connection.podcast_title);
    }
    
    nodes.get(connection.source_id).val++;
    nodes.get(connection.target_id).val++;
    
    links.push({
      source: connection.source_id,
      target: connection.target_id,
      value: connection.episodes_together,
      podcast: connection.podcast_title
    });
  });

  // Convert Sets to Arrays for easier handling
  nodes.forEach(node => {
    node.podcasts = Array.from(node.podcasts);
  });

  return {
    nodes: Array.from(nodes.values()),
    links: links
  };
};

// Add custom forces to handle podcast-based clustering
useEffect(() => {
  if (graphData.nodes.length) {
    // Get unique podcasts
    const allPodcasts = new Set(
      graphData.nodes.flatMap(node => node.podcasts)
    );

    // Create cluster centers
    const numPodcasts = allPodcasts.size;
    const radius = Math.min(dimensions.width, dimensions.height) * 0.4;
    const podcastCenters = {};
    
    Array.from(allPodcasts).forEach((podcast, i) => {
      const angle = (2 * Math.PI * i) / numPodcasts;
      podcastCenters[podcast] = {
        x: dimensions.width/2 + radius * Math.cos(angle),
        y: dimensions.height/2 + radius * Math.sin(angle)
      };
    });

    // Custom force to pull nodes towards their podcast centers
    const clusterForce = (alpha) => {
      graphData.nodes.forEach(node => {
        if (node.podcasts && node.podcasts.length > 0) {
          // Calculate average position of all podcast centers this node belongs to
          const avgCenter = node.podcasts.reduce((acc, podcast) => {
            const center = podcastCenters[podcast];
            acc.x += center.x;
            acc.y += center.y;
            return acc;
          }, { x: 0, y: 0 });
          
          avgCenter.x /= node.podcasts.length;
          avgCenter.y /= node.podcasts.length;

          node.vx += (avgCenter.x - node.x) * alpha * 0.5;
          node.vy += (avgCenter.y - node.y) * alpha * 0.5;
        }
      });
    };

    const simulation = forceSimulation(graphData.nodes)
      .force('link', forceLink(graphData.links).id(d => d.id).distance(100))
      .force('charge', forceManyBody().strength(-1000))
      .force('center', forceCenter(dimensions.width/2, dimensions.height/2))
      .force('collide', forceCollide().radius(d => Math.sqrt(d.val * 100) + 20))
      .force('cluster', clusterForce)
      .alpha(1)
      .alphaDecay(0.01);

    return () => simulation.stop();
  }
}, [graphData, dimensions]);

// Update the force simulation useEffect to use filteredGraphData
useEffect(() => {
  if (filteredGraphData.nodes.length) {
    const simulation = forceSimulation(filteredGraphData.nodes)
      .force('link', forceLink(filteredGraphData.links).id(d => d.id).distance(50))  // Increased distance
      .force('charge', forceManyBody().strength(-1000))  // Increased repulsion
      .force('center', forceCenter(dimensions.width/2, dimensions.height/2))
      .force('collide', forceCollide().radius(d => Math.sqrt(d.val * 100) + 30))  // Increased collision radius
      .alpha(1)  // Reset alpha to reheat the simulation
      .alphaDecay(0.01); // Slower cool-down

    return () => simulation.stop();
  }
}, [filteredGraphData, dimensions]);


  // Loading and error states
  if (loading) return (
    <div className="flex items-center justify-center h-screen">
      <div className="text-xl font-semibold">Loading network data...</div>
    </div>
  );

  if (error) return (
    <div className="flex items-center justify-center h-screen text-red-500">
      <div className="text-xl font-semibold">Error: {error}</div>
    </div>
  );



return (
    <div className="flex h-screen w-full relative">
      {/* Fixed-width sidebar */}
      <div className="w-96 min-w-[24rem] bg-gray-50 p-4 overflow-y-auto shadow-lg z-10">
        <div className="mb-4">
          <h2 className="text-2xl font-bold text-gray-800">Podcast Network</h2>
          <p className="text-gray-600">Explore host connections and collaborations</p>
        </div>

        {selectedNode ? (
          <HostProfileCard 
            host={selectedNode} 
            connections={selectedNodeConnections}
            onClose={() => {
              setSelectedNode(null);
              setSelectedNodeConnections([]);
              setHighlightNodes(new Set());
              setHighlightLinks(new Set());
              setSelectedLinks(new Set());
            }}
          />
        ) : selectedLink ? (
          <ConnectionDetails 
            connection={selectedLink}
            onClose={() => {
              setSelectedLink(null);
              setHighlightNodes(new Set());
              setHighlightLinks(new Set());
              setSelectedLinks(new Set());
            }}
          />
        ) : (
          <FilterPanel 
            onFiltersChange={filters => {
              const newFilters = {
                ...currentFilters,  // Keep existing filters
                ...filters         // Apply new changes
              };
              setCurrentFilters(newFilters);
              handleFiltersChange(newFilters);  // Pass ALL filters
            }}
            networkStats={networkStats}
            currentFilters={currentFilters}
          />
        )}
      </div>


      {/* Graph container with proper sizing */}
      <div className="flex-1 ">
        {/* Loading filter overlay */}
        {isFilteringGraph && <LoadingOverlay />}

        <ForceGraph2D
          graphData={filteredGraphData}
          // Node appearance
          
          nodeRelSize={6}  // Slightly smaller nodes
          nodeVal={node => Math.sqrt(node.val * 50)}  // Adjusted node size scaling
          nodeLabel={node => node.name}

          nodeColor={node => {
            if (selectedNode && node.id === selectedNode.id) {
              return '#f59e0b' // bright red for selected node
            }
            return highlightNodes.has(node.id) ? '#f59e0b' : '#3b82f6'
          }}

          nodeCanvasObject={(node, ctx, globalScale) => {
          // Draw circle background
          
          const size = 10 * Math.sqrt(node.val);
          const borderWidth = 0.5;
          ctx.beginPath();
          ctx.arc(node.x, node.y, size + borderWidth, 0, 2 * Math.PI, false);
          ctx.fillStyle = selectedNode && node.id === selectedNode.id 
            ? '#dc2626' // red border for selected
            : highlightNodes.has(node.id) 
              ? '#f59e0b' // orange border for highlighted
              : '#94a3b8'; // gray border for default
          ctx.fill();

          // Draw cluster indicator EF: this seems very weak...
          if (node.podcasts && node.podcasts.length > 0) {
            ctx.beginPath();
            ctx.arc(node.x, node.y, size + borderWidth + 4, 0, 2 * Math.PI, false);
            ctx.fillStyle = `hsla(${node.podcasts[0].length * 7}, 70%, 50%, 0.4)`;
            ctx.fill();
          }

          
          const img = new Image();
          img.src = node.image || getAvatarUrl(node.name);

          
          // Create circular clip for image
          ctx.save();
          ctx.beginPath();
          ctx.arc(node.x, node.y, size - 1, 0, 2 * Math.PI, false);
          ctx.clip();
          
          // Draw the image
          ctx.drawImage(
            img,
            node.x - size + 1,
            node.y - size + 1,
            size * 2 - 2,
            size * 2 - 2
          );
          ctx.restore();
          

          // Draw label if zoomed in enough
          const label = node.name;
          if (globalScale >= 1) {
            ctx.font = '8px Arial';
            ctx.textAlign = 'center';
            ctx.textBaseline = 'middle';
            ctx.fillStyle = 'white';
            ctx.fillText(label, node.x, node.y + 10);
          }
          }}
          nodePointerAreaPaint={(node, color, ctx) => {
            ctx.beginPath();
            ctx.arc(node.x, node.y, 5 * Math.sqrt(node.val), 0, 2 * Math.PI, false);
            ctx.fillStyle = color;
            ctx.fill();
          }}
          // Remove default node rendering
          nodeRelSize={0}

          
          // Link appearance
            linkLabel={link => `${link.value} episodes together on ${link.podcast}`}
            linkWidth={link => {
              const minWidth = 1;
              const maxWidth = 10; // 5 was default, EF bumped up to 10 to try
              const episodeScale = link.value / 2; // Adjust divisor to control scaling
              const width = Math.min(maxWidth, Math.max(minWidth, episodeScale));
              
              // Make selected/highlighted links even thicker
              return (selectedLinks.has(link) || highlightLinks.has(link)) 
                ? width + 2 
                : width;
            }}
            linkColor={link => 
              selectedLinks.has(link) || highlightLinks.has(link) 
                ? '#f59e0b' 
                : `rgba(156, 163, 175, ${Math.min(1, link.value / 3)})`  // Opacity based on episodes
            }
          linkDirectionalParticles={link => 
            selectedLinks.has(link) || highlightLinks.has(link) ? 4 : 0
          }
          
          // Force simulation parameters
          d3Force="charge"
          d3ForceStrength={-500} // Stronger repulsion between nodes
          d3AlphaDecay={0.01} // Slower simulation cool-down
          d3VelocityDecay={0.9} // More movement damping
          
          // Link force parameters
          linkDistance={50} // Fixed distance between connected nodes
          linkStrength={0.8} // How rigid the links are
          
          // Center force to keep graph centered
          centerStrength={0.3}
          
          // Particles for highlighted links
          linkDirectionalParticles={link => 
            highlightLinks.has(link) ? 4 : 0
          }
          linkDirectionalParticleWidth={2}
          
          // Graph size
          width={dimensions.width}
          height={dimensions.height}
          
          // Interaction
          enableZoomInteraction={true}
          enablePanInteraction={true}
          minZoom={0.1}
          maxZoom={2.5}
          
          // Event handlers
          onNodeHover={handleNodeHover}
          onNodeClick={handleNodeClick}
          onLinkClick={handleLinkClick}
          
          // Additional forces for better spacing
          d3Force="collide"
          d3ForceCollide={node => Math.sqrt(node.val * 100) + 20} // Prevent node overlap
        />
      </div>
      <PodcastLegend 
        podcasts={Array.from(new Set(graphData.nodes.flatMap(n => n.podcasts)))} 
      />
    </div>
  );
};

export default PodcastHostNetwork;